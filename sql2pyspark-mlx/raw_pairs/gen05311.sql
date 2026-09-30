SELECT
    id,
    paid_at,
    method,
    status
FROM payments
WHERE status <> 'REFUNDED'
