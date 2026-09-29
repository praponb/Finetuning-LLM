SELECT o.id, o.order_date, o.store_id
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)
ORDER BY order_date, id
LIMIT 10
