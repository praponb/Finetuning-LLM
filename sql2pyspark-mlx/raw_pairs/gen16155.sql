SELECT p.id, p.launch_date FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE rating <= 4.0)
