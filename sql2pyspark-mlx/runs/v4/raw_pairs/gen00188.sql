SELECT id, sale_date, revenue FROM store_sales WHERE sale_date < (SELECT MIN(sale_date) FROM store_sales WHERE store_id = 6);
