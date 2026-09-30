SELECT
    oi.id,
    oi.unit_price,
    oi.quantity,
    p.price,
    p.brand,
    p.stock_qty
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
WHERE p.supplier_id IS NOT NULL
