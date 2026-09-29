SELECT
    id,
    status,
    amount,
    order_id,
    FIRST_VALUE(order_id) OVER (PARTITION BY status ORDER BY amount, id) AS first_order_id
FROM payments
WHERE order_id = 31
ORDER BY first_order_id DESC
