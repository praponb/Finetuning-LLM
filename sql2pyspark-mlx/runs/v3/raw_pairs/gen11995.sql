SELECT p.id, p.category FROM products p WHERE p.id IN (SELECT product_id FROM inventory) ORDER BY id DESC;
