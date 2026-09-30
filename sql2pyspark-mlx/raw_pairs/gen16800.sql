SELECT p.id, p.product_name FROM products p WHERE p.price >= 1190 AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id)
