SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    p.brand,
    p.category,
    p.stock_qty,
    ROUND(p.cost * 100, 2) AS cost_scaled
FROM order_items oi
RIGHT JOIN products p ON p.id = oi.product_id
WHERE NOT (oi.product_id IS NOT NULL AND oi.order_id IN (25, 28, 32, 37));
