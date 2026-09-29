SELECT ss.id, ss.store_id FROM store_sales ss WHERE ss.product_id NOT IN (SELECT id FROM products WHERE cost BETWEEN 520 AND 700);
