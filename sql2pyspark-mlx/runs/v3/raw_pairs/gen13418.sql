select id, sale_date, units, store_id from store_sales where sale_date = (select max(sale_date) from store_sales where revenue <= 2600);
