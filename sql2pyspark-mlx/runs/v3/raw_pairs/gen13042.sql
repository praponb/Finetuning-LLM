SELECT CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort, s.region, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(o.status))) status_values
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
WHERE o.channel = 'partner'
GROUP BY CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END, s.region
ORDER BY opened_cohort;
