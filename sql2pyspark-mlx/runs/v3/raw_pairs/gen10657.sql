SELECT DATE_TRUNC('MONTH', p.launch_date) AS launch_month, AVG(ss.revenue) AS avg_revenue, MAX(ss.revenue) AS max_revenue
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
JOIN products p ON ss.product_id = p.id
GROUP BY DATE_TRUNC('MONTH', p.launch_date);
