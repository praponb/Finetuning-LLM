SELECT i.id, i.quantity
FROM inventory i
WHERE i.product_id IN (SELECT id FROM products WHERE stock_qty >= 199);
