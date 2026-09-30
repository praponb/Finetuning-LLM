SELECT CASE WHEN status IN ('RETURNED', 'PENDING') THEN 'tier_1' ELSE 'other' END AS status_group, SUM(amount) AS total_amount FROM orders GROUP BY 1 ORDER BY status_group, 2
