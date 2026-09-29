SELECT QUARTER(sale_date) AS sale_quarter, COUNT(units) AS units_count
FROM store_sales
GROUP BY sale_quarter
ORDER BY sale_quarter DESC
LIMIT 1
