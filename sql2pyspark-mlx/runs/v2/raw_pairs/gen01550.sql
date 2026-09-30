SELECT
    p.id,
    p.stock_qty,
    p.cost,
    p.brand
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id);
