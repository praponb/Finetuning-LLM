SELECT
    o.id,
    o.customer_id,
    o.status,
    o.store_id
FROM orders o
WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.method IN ('paypal', 'card', 'bank_transfer'))
ORDER BY store_id DESC, status, id
LIMIT 1
