SELECT
    oi.id,
    oi.order_id,
    o.customer_id,
    o.status,
    ROUND(oi.quantity, 1) AS quantity_rounded
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
WHERE oi.quantity BETWEEN 6 AND 7
ORDER BY o.status DESC, id ASC
