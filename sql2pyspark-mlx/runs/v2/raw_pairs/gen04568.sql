SELECT
    id,
    amount,
    paid_at,
    order_id,
    CEIL(amount) AS amount_whole
FROM payments
WHERE paid_at >= '2024-09-01' AND HOUR(paid_at) BETWEEN 7 AND 14
ORDER BY paid_at DESC
