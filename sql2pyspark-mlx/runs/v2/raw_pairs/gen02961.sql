select brand, price_count from (select brand, count(price) as price_count from products group by brand) as t where price_count >= 9;
