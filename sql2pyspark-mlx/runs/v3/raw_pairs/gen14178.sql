SELECT
    oi.id,
    oi.order_id,
    oi.unit_price,
    oi.quantity,
    p.price,
    p.supplier_id
FROM order_items oi
JOIN products p ON p.id = oi.product_id
WHERE SIZE(p.tags) > 2 AND p.stock_qty < 167
ORDER BY p.supplier_id DESC, id
LIMIT 5;
