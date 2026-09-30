SELECT
    p.id,
    p.status,
    p.method,
    p.order_id,
    o.store_id,
    o.customer_id,
    o.status order_status
FROM payments p
JOIN orders o ON p.order_id = o.id
WHERE o.store_id IN (27, 30) AND p.amount <= 1530;
