SELECT
    oi.id,
    oi.product_id,
    o.store_id,
    o.discount,
    p.stock_qty,
    p.cost
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
JOIN products p ON p.id = oi.product_id
ORDER BY discount DESC, id
LIMIT 1
