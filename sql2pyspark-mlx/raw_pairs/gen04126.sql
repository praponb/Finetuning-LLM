SELECT
    o.id,
    o.shipping_fee,
    o.discount,
    o.store_id
FROM orders o
WHERE o.customer_id = 5 AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.paid_at < '2023-05-03')
ORDER BY o.shipping_fee DESC
