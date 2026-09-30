SELECT MONTH(sale_date) AS sale_month, MAX(units) - MIN(units) AS units_range
FROM store_sales
WHERE YEAR(sale_date) >= 2025
GROUP BY MONTH(sale_date)
HAVING MAX(units) - MIN(units) >= 47
ORDER BY units_range;
