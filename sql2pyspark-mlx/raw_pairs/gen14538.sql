SELECT p.id, p.product_name, p.price FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating >= 3.6) ORDER BY p.price DESC
