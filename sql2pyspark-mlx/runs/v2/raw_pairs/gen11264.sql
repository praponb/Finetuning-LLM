SELECT id, store_id, sale_date, DATE_TRUNC('YEAR', sale_date) AS sale_date_period FROM store_sales WHERE sale_date > '2023-08-01' AND revenue <= 1500 AND MONTH(sale_date) IN (2, 7, 12);
