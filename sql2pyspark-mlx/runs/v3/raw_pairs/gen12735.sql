SELECT sale_date, store_id, DATE_TRUNC('MONTH', sale_date) AS sale_date_period FROM store_sales WHERE (revenue BETWEEN 3400 AND 3900 AND sale_date > '2024-06-01') OR revenue BETWEEN 1300 AND 1800
