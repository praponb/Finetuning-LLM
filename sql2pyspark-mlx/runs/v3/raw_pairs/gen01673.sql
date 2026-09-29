SELECT CASE WHEN method IN ('paypal', 'cash') THEN 'core' ELSE 'other' END AS method_group, method, COUNT(DISTINCT amount) AS unique_methods
FROM payments
WHERE status NOT IN ('FAILED', 'SUCCESS', 'REFUNDED') OR paid_at >= '2024-12-01'
GROUP BY 1, 2
HAVING unique_methods < 5
