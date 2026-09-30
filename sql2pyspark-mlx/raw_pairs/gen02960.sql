SELECT
    CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
    s.region,
    COUNT(DISTINCT i.quantity) AS unique_stores,
    SUM(i.quantity) AS total_quantity
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
GROUP BY CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END, s.region
ORDER BY total_quantity, s.region DESC, opened_cohort
LIMIT 10;
