SELECT o.store_id, CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, SUM(CASE WHEN p.status = 'FAILED' THEN p.amount ELSE 0 END) AS failed_amount
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
GROUP BY o.store_id, CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END;
