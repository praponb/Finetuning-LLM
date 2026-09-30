SELECT id, store_id, revenue, sale_date, FIRST_VALUE(sale_date) OVER (PARTITION BY store_id ORDER BY revenue, id) first_sale_date FROM store_sales;
