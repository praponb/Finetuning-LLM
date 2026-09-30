SELECT
    oi.id,
    oi.quantity,
    oi.order_id,
    p.stock_qty,
    p.supplier_id,
    s.rating
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
