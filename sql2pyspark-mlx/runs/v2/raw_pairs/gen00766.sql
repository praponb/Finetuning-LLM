SELECT
    payments.method,
    payments.paid_at,
    orders.shipping_fee,
    customers.email,
    ROUND(orders.shipping_fee, 2) AS shipping_fee_rounded
FROM payments
INNER JOIN orders ON orders.id = payments.order_id
LEFT JOIN customers ON orders.customer_id = customers.id
ORDER BY shipping_fee DESC;
