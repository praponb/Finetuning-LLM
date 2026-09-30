SELECT sale_date, revenue, DATE_TRUNC('MONTH', sale_date) AS sale_date_period
FROM store_sales
WHERE revenue IS NOT NULL;
