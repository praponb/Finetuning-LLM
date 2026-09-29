SELECT CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END launch_cohort, AVG(ss.revenue) avg_revenue, MAX(ss.revenue) max_revenue
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY max_revenue DESC;
