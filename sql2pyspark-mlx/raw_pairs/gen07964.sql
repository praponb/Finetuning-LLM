SELECT
    o.id,
    o.order_date,
    o.status,
    o.shipping_fee
FROM orders o
WHERE o.store_id IS NULL AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id)
ORDER BY id, o.status;
