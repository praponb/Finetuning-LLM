SELECT
    id,
    status,
    order_id,
    amount,
    FLOOR(amount) AS amount_whole
FROM payments
WHERE order_id IN (4, 15, 37)
