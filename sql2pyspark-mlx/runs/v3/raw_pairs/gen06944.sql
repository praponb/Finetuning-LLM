select id, product_id, date_trunc('WEEK', sale_date) as sale_date_period from store_sales where product_id in (3, 10) order by product_id, id limit 1
