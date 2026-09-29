SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    o.discount,
    p.price
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
LEFT JOIN products p ON p.id = oi.product_id
WHERE oi.unit_price <= 120 OR o.store_id IS NOT NULL;
