SELECT p.id, p.stock_qty, p.launch_date
FROM products p
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id)
