SELECT CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, SUM(p.amount) AS total_amount, SUM(CASE WHEN p.status = 'REFUNDED' THEN p.amount ELSE 0 END) AS refunded_amount
FROM payments p
JOIN orders o ON p.order_id = o.id
JOIN stores s ON o.store_id = s.id
WHERE HOUR(p.paid_at) BETWEEN 9 AND 17
GROUP BY CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
