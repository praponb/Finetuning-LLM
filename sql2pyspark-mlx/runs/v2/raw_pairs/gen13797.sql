SELECT o.id, o.status, o.store_id
FROM orders o
WHERE o.status != 'CANCELLED' AND o.customer_id NOT IN (SELECT id FROM customers WHERE is_active = TRUE)
ORDER BY o.store_id DESC, o.status, id
LIMIT 5;
