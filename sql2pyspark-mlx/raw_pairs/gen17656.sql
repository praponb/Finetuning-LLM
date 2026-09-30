SELECT
    oi.quantity,
    oi.product_id,
    p.supplier_id,
    o.discount,
    o.order_date
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON o.id = oi.order_id
WHERE p.stock_qty IS NULL OR oi.order_id IN (13, 37)
ORDER BY o.discount DESC, supplier_id DESC;
