SELECT
    p.id,
    p.supplier_id,
    p.launch_date,
    p.stock_qty
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.product_id = 38)
ORDER BY id
