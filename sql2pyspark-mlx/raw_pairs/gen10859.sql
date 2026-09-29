SELECT revenue, sale_date, DATE_TRUNC('WEEK', sale_date) AS sale_date_period
FROM store_sales
ORDER BY sale_date_period
