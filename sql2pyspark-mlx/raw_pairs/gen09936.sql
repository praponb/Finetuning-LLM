select brand, avg_price from (select brand, round(avg(price), 2) as avg_price from products where supplier_id is null group by brand) as s where avg_price < 51 order by brand asc;
