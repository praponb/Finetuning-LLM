SELECT
    HOUR(paid_at) AS paid_hour,
    QUARTER(paid_at) AS paid_quarter,
    status,
    MIN(paid_at) AS min_paid_at
FROM payments
WHERE order_id = 26
GROUP BY 1, 2, 3
ORDER BY status
