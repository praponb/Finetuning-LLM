SELECT
    oi.id,
    oi.unit_price,
    oi.quantity,
    p.stock_qty,
    p.category,
    s.city
FROM order_items oi
LEFT JOIN products p ON p.id = oi.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.product_name IS NULL AND p.cost > 370
