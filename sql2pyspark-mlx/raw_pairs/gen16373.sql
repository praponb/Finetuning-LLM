SELECT
    payments.paid_at,
    orders.order_date,
    orders.channel,
    orders.shipping_fee
FROM payments
LEFT JOIN orders ON orders.id = payments.order_id
WHERE orders.discount > 0.36 AND orders.store_id = 12
ORDER BY paid_at DESC
