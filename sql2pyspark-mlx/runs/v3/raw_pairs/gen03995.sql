SELECT
    o.id,
    o.amount,
    o.customer_id,
    o.shipping_fee
FROM orders o
WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id)
