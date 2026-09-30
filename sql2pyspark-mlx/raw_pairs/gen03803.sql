SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, ROUND(AVG(ss.revenue), 2) AS avg_revenue, ROUND(AVG(ss.units), 2) AS avg_units
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
JOIN stores s ON ss.store_id = s.id
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
ORDER BY 3 DESC;
