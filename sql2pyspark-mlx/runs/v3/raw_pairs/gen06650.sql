SELECT DATE_TRUNC('MONTH', sale_date) AS sale_month, COUNT(DISTINCT revenue) AS unique_stores
FROM store_sales
GROUP BY DATE_TRUNC('MONTH', sale_date)
HAVING COUNT(DISTINCT revenue) > 4;
