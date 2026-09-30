SELECT
    payments.id,
    payments.method,
    payments.status,
    orders.amount,
    orders.discount
FROM payments
FULL OUTER JOIN orders ON payments.order_id = orders.id
WHERE payments.order_id IS NOT NULL
ORDER BY status ASC
