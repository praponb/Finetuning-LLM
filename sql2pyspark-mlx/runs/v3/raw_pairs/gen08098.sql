SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    oi.unit_price
FROM order_items oi
WHERE oi.product_id IS NOT NULL AND oi.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee > 30)
ORDER BY oi.unit_price;
