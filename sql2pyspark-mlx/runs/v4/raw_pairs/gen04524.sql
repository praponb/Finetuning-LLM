with top_products as (select *, row_number() over (partition by brand order by price desc, id) as row_num from products) select id, brand, price from top_products where row_num <= 3 order by brand;
