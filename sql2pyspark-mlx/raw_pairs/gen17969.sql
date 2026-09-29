SELECT
    p.id,
    p.order_id,
    o.store_id,
    o.status
FROM payments p
RIGHT JOIN orders o ON p.order_id = o.id
WHERE p.status = 'FAILED'
ORDER BY id DESC
