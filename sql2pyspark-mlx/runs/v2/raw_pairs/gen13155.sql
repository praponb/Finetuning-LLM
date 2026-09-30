SELECT
    oi.id,
    oi.quantity,
    p.stock_qty,
    p.supplier_id,
    o.status
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON o.id = oi.order_id
ORDER BY id DESC, supplier_id;
