SELECT
    oi.quantity,
    p.stock_qty,
    p.supplier_id,
    s.country,
    s.rating
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE oi.order_id IN (5, 35, 40)
ORDER BY 1;
