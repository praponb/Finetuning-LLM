SELECT
    oi.id,
    oi.unit_price,
    oi.quantity,
    oi.product_id
FROM order_items oi
WHERE oi.order_id IN (2, 7, 18, 29) AND oi.product_id IN (SELECT id FROM products WHERE supplier_id IN (15, 27, 37));
