SELECT CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, COUNT(CASE WHEN o.channel = 'partner' THEN 1 END) AS partner_count
FROM orders o
LEFT JOIN stores s ON s.id = o.store_id
RIGHT JOIN customers c ON o.customer_id = c.id
GROUP BY CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY partner_count, opened_cohort
LIMIT 1;
