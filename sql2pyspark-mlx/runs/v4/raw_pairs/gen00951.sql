SELECT i.id, i.quantity, i.store_id
FROM inventory i
WHERE i.quantity IS NOT NULL AND i.product_id NOT IN (SELECT id FROM products WHERE stock_qty != 399)
ORDER BY quantity DESC
