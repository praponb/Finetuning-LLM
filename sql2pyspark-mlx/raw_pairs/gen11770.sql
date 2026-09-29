SELECT p.id, p.price, p.stock_qty, p.cost FROM products p WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id)
