SELECT p.id, p.product_name, COUNT(oi.id) order_item_count
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
WHERE p.stock_qty = 217
GROUP BY p.id, p.product_name
