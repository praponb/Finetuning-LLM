SELECT DATE_TRUNC('MONTH', sale_date) AS sale_month, SUM(units) AS total_units, ROUND(AVG(units), 2) AS avg_units
FROM store_sales
WHERE revenue BETWEEN 2500 AND 3000 AND units < 27
GROUP BY 1
ORDER BY avg_units DESC, total_units, sale_month
LIMIT 3
