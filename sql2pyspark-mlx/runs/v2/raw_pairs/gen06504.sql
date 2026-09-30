SELECT DATE_TRUNC('MONTH', p.launch_date) launch_month, COUNT(*) total_count, COUNT(ss.units) units_count
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
LEFT JOIN products p ON ss.product_id = p.id
WHERE s.store_name NOT LIKE '%Cent%'
GROUP BY DATE_TRUNC('MONTH', p.launch_date)
