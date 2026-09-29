SELECT p.id, p.status
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE store_id IS NULL)
ORDER BY status, id DESC
