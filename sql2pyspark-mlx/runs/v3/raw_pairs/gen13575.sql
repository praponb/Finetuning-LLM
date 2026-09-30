SELECT CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, MAX(s.shipped_date) AS last_shipped_date, COUNT(s.cost) AS cost_count
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
INNER JOIN customers c ON c.id = o.customer_id
GROUP BY CASE WHEN YEAR(c.signup_date) < 2024 THEN 'before_2024' WHEN YEAR(c.signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
