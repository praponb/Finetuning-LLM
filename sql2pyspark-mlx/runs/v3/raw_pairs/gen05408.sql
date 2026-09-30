SELECT
    id,
    status,
    amount,
    MIN(amount) OVER (PARTITION BY status) AS status_min_amount
FROM payments
WHERE paid_at < '2024-05-01'
