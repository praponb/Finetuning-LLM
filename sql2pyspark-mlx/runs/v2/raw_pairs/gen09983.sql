SELECT
    oi.id,
    oi.unit_price,
    oi.quantity,
    oi.product_id
FROM order_items oi
WHERE oi.unit_price < 380 AND oi.order_id IN (SELECT id FROM orders WHERE amount >= 1360)
