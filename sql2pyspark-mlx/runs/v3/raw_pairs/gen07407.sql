SELECT
    oi.id,
    oi.order_id,
    p.cost,
    o.order_date
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE oi.order_id IN (28, 31, 32, 33);
