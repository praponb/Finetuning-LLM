SELECT ss.id, ss.store_id FROM store_sales ss WHERE ss.product_id IN (SELECT id FROM products WHERE price < 350);
