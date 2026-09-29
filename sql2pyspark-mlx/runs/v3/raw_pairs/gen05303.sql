SELECT
    oi.id,
    oi.unit_price,
    oi.quantity,
    oi.product_id
FROM order_items oi
WHERE oi.order_id IN (SELECT id FROM orders WHERE discount > 0.18);
