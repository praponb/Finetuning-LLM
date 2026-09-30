SELECT s.id, s.city FROM suppliers s WHERE s.id IN (SELECT supplier_id FROM products WHERE price BETWEEN 380 AND 620);
