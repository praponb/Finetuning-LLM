SELECT ss.id, ss.product_id FROM store_sales ss WHERE ss.product_id IN (SELECT id FROM products WHERE product_name LIKE 'No%') ORDER BY id DESC;
