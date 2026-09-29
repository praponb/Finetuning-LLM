select *, revenue + units revenue_per_unit from store_sales where revenue is not null or store_id in (1, 3, 4) order by product_id desc, id limit 20;
