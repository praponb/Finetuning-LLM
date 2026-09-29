SELECT id, sale_date, store_id, DATE_TRUNC('MONTH', sale_date) AS sale_date_period FROM store_sales;
