SELECT
    oi.id,
    oi.product_id,
    oi.quantity,
    oi.order_id
FROM order_items oi
WHERE oi.quantity > 6 AND oi.order_id IN (SELECT id FROM orders WHERE discount BETWEEN 0.12 AND 0.39)
