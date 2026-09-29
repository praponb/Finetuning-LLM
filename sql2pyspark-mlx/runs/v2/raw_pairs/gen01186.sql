SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    oi.unit_price
FROM order_items oi
WHERE oi.order_id IN (SELECT id FROM orders WHERE channel != 'mobile')
