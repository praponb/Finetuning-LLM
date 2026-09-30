SELECT p.id, p.stock_qty FROM products p WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.store_id = 5) ORDER BY p.stock_qty DESC
