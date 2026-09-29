SELECT CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, SUM(i.quantity) AS total_quantity, MIN(i.quantity) AS min_quantity
FROM inventory i
LEFT JOIN stores s ON s.id = i.store_id
LEFT JOIN products p ON i.product_id = p.id
GROUP BY CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING SUM(i.quantity) > 800;
