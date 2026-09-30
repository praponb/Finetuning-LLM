SELECT
    oi.quantity,
    oi.order_id,
    oi.unit_price,
    o.status,
    o.store_id,
    o.customer_id
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price >= 300
ORDER BY unit_price DESC;
