SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    oi.quantity,
    p.category,
    p.brand
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
WHERE oi.order_id IN (4, 18, 38) AND oi.quantity < 5;
