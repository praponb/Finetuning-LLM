select id, sale_date, product_id from store_sales where sale_date = (select min(sale_date) from store_sales where store_id = 6) order by id desc;
