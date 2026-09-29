SELECT
    id,
    method,
    amount,
    status,
    paid_at,
    RANK() OVER (PARTITION BY method ORDER BY amount DESC) AS rnk,
    RANK() OVER (PARTITION BY status ORDER BY paid_at DESC) AS rnk_2
FROM payments
WHERE paid_at >= '2024-04-01'
ORDER BY paid_at
