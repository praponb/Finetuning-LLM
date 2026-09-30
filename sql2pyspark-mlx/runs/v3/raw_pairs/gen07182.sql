SELECT product_id, DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, SUM(units) AS total_units
FROM store_sales
GROUP BY product_id, DATE_FORMAT(sale_date, 'yyyy-MM')
HAVING SUM(units) > 200
ORDER BY 2 ASC;
