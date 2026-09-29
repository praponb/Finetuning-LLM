SELECT
    id,
    status,
    method,
    DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label
FROM payments
WHERE status NOT IN ('REFUNDED', 'FAILED')
ORDER BY 1
LIMIT 10
