SELECT o.id, o.status, o.shipping_fee
FROM orders o
WHERE o.store_id = 37 AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)
ORDER BY o.shipping_fee DESC
