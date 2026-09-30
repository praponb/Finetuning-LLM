SELECT store_id, DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, AVG(units) AS avg_units
FROM store_sales
WHERE revenue BETWEEN 1400 AND 2800
GROUP BY store_id, DATE_FORMAT(sale_date, 'yyyy-MM')
ORDER BY sale_month, store_id DESC
