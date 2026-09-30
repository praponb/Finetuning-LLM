SELECT
    method,
    status,
    CASE WHEN amount >= 1480 THEN 'high' WHEN amount >= 300 THEN 'mid' ELSE 'low' END AS amount_bucket,
    MAX(amount) - MIN(amount) AS amount_range,
    COUNT(amount) AS amount_count
FROM payments
WHERE paid_at < '2025-02-13'
GROUP BY 1, 2, 3
