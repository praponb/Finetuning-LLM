SELECT p.id, p.stock_qty
FROM products p
WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.order_id IN (12, 26, 30, 34));
