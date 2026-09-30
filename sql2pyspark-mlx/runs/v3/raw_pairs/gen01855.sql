select id, revenue, product_id, sale_date from store_sales where revenue < (select min(revenue) from store_sales) order by sale_date, id limit 50;
