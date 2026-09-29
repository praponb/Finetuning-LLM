SELECT DATE_TRUNC('MONTH', p.launch_date) AS launch_month, COUNT(ss.revenue) AS revenue_count, SUM(ss.units) AS total_units
FROM store_sales ss
RIGHT OUTER JOIN products p ON p.id = ss.product_id
LEFT OUTER JOIN stores s ON ss.store_id = s.id
GROUP BY DATE_TRUNC('MONTH', p.launch_date)
HAVING revenue_count >= 3
