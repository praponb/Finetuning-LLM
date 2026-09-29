SELECT
    oi.id,
    oi.unit_price,
    oi.quantity,
    oi.product_id
FROM order_items oi
WHERE oi.product_id IN (7, 25) AND oi.order_id IN (SELECT id FROM orders WHERE store_id = 9)
ORDER BY oi.id;
