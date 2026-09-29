select * from (select category, brand, price from products where product_name like 'Bac%') pivot (count(price) for brand in ('Stark', 'Acme')) order by category;
