SELECT p.id, p.price, p.launch_date FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating BETWEEN 2.9 AND 4.1)
