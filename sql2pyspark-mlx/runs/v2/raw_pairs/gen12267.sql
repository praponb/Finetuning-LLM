SELECT
    p.paid_at,
    p.order_id,
    o.status,
    o.store_id
FROM payments p
INNER JOIN orders o ON o.id = p.order_id;
