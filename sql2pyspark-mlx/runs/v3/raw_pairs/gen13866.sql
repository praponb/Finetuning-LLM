SELECT CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, AVG(ss.units) AS avg_units
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END;
