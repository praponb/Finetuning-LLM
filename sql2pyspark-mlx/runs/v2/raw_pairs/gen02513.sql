SELECT p.id, p.stock_qty, p.category
FROM products p
WHERE p.id IN (SELECT product_id FROM order_items)
