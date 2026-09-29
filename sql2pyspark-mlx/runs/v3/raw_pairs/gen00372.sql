SELECT
    o.id,
    o.order_date,
    o.store_id,
    o.amount
FROM orders o
WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.order_id IS NOT NULL)
ORDER BY o.amount;
