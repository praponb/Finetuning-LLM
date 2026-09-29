with top_products as (select id, brand, price, row_number() over (partition by brand order by price, id) as row_num from products) select * from top_products where row_num <= 3 order by 2
