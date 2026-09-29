SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, ROUND(AVG(units), 2) AS avg_units, COUNT(*) AS cnt
FROM store_sales
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
