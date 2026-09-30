SELECT CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, p.brand, COUNT(i.quantity) AS quantity_count
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
FULL OUTER JOIN stores s ON s.id = i.store_id
GROUP BY CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END, p.brand;
