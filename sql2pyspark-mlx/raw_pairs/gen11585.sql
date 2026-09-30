SELECT
    oi.id,
    oi.quantity,
    oi.unit_price,
    oi.order_id,
    o.order_date,
    o.customer_id
FROM order_items oi
RIGHT OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.order_id IN (18, 20, 37)
ORDER BY quantity DESC;
