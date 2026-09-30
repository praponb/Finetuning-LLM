SELECT CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, MIN(p.paid_at) AS first_paid_at, MAX(p.paid_at) AS latest_paid_at
FROM payments p
LEFT OUTER JOIN orders o ON p.order_id = o.id
WHERE o.discount < 0.34
GROUP BY CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY first_paid_at DESC, order_cohort
LIMIT 3
