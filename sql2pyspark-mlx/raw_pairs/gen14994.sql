SELECT
    p.id,
    p.cost,
    p.stock_qty,
    p.launch_date
FROM products p
WHERE p.stock_qty = 121 AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.quantity >= 4);
