SELECT p.id, p.price, p.product_name, p.cost FROM products p WHERE p.product_name LIKE '%Off%' AND p.supplier_id IN (SELECT id FROM suppliers WHERE rating < 4.0) ORDER BY product_name, id LIMIT 20;
