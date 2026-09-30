SELECT
    status,
    method,
    ROUND((100 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_failed,
    MAX(amount) AS max_amount
FROM payments
WHERE paid_at >= '2023-03-18' AND order_id = 12
GROUP BY status, method
ORDER BY status DESC
