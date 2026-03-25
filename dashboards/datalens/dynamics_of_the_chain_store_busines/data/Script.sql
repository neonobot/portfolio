--Сырые данные хранятся в базе default
--Из csv файлов были загружены две таблицы: default.district и default.orders (все csv файлы лежат вместе с sql-скриптом в data)
--При импорте из csv файла нужно использовать: Encoding[cp1251] и Column delimiter[;] 


--Создание роли для BI-системы
create role bisystem;

--Временно даем ей доступ ко всем таблицам
grant select on *.* to bisystem;
show grants for bisystem;

--Создание нового юзера для Yandex Datalens (аутентификация по технологии sha256)
create user datalens default role bisystem identified with sha256_password by 'datalens';


--Создание БД для словарей, предобработанных данных и витрин
create database dictionary; -- из csv файла загружаем таблицу dictionary.dict_district
create database calc;
create database cdm;

--Забираем у роли bisystem право на обращение ко всем БД, оставляем возможность обращаться к БД с витринами
revoke select on *.* from bisystem;
grant select on cdm.* to bisystem;

--Создание таблицы с предобработанными данными
create table calc.orders_full engine=MergeTree order by orderid as
select t1.*, t2.* except(orderid)
from orders t1
left join district t2 on t1.orderid = t2.orderid;


--Создание витрин данных
--Для вкладки "Динамика бизнеса"
create table cdm.orders_calc engine=MergeTree order by data_ as 
select data_
	, sum(receipt) sum_sales
	, count(orderid) uniq_ordres
	, round(sum_sales/uniq_ordres) avg_receipt
	, max(receipt) max_reciept
	, min(receipt) min_reciept
from 
(
	select toDate(parseDateTimeBestEffort(orderdate)) as data_
		, orderid
		, sum(sales) as receipt
	from calc.orders_full
	group by data_, orderid
)
group by data_;

select * from cdm.orders_calc;

--Для вкладки "Рейтинг магазинов"
create table cdm.shop_rating engine=MergeTree order by shopname as
select shopname
	, uniq(orderid) uniq_orders
	, sum(sales) sum_sales
	, round(sum_sales/uniq_orders) avg_receipt
from calc.orders_full
group by shopname 
order by sum_sales desc;

select * from cdm.shop_rating;

--Для вкладки "Продажи по территории"
create temporary table t1 as 
select deliverydistrictname
	, sum(sales) sum_sales
	, uniq(orderid) uniq_orders
	, round(sum_sales/uniq_orders) avg_receipt
from calc.orders_full 
group by deliverydistrictname;

create table cdm.area_sales engine=MergeTree order by deliverydistrictname as
select t1.*, t2.* except (name)
from t1
left any join 
(
	select name
		, abbrev_ao
	from dictionary.dict_district
) t2
on t1.deliverydistrictname = t2.name;

select * from cdm.area_sales;
