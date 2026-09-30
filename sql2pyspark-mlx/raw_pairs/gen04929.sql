SELECT
    id,
    status,
    paid_at,
    amount,
    order_id,
    method,
    amount - LAG(amount, 1) OVER (PARTITION BY status ORDER BY paid_at, id) AS amount_change,
    FIRST_VALUE(method) OVER (PARTITION BY status, order_id ORDER BY amount DESC, id) AS first_method
FROM payments
WHERE paid_at >= '2023-03-01'
ORDER BY amount_change
