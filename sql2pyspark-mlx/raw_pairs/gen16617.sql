SELECT o.id, o.discount, o.store_id
FROM orders o
WHERE o.store_id IS NULL AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND HOUR(p.paid_at) BETWEEN 12 AND 15)
