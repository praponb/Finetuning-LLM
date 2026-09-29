SELECT
    payments.id,
    payments.amount,
    payments.status,
    payments.paid_at,
    orders.status order_status,
    orders.amount order_amount,
    orders.shipping_fee
FROM payments
INNER JOIN orders ON orders.id = payments.order_id
WHERE TO_DATE(payments.paid_at) < '2023-10-04'
ORDER BY 4 DESC
