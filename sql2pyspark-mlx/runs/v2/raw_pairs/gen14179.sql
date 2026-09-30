SELECT
    oi.id,
    oi.quantity,
    o.store_id,
    o.order_date,
    o.status
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
WHERE o.status = 'SHIPPED' AND o.discount IS NOT NULL
