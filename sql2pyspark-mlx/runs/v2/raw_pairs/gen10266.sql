select id, product_id, sale_date, ntile(10) over (partition by product_id order by sale_date, id) as bucket from store_sales;
