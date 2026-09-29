SELECT p.id, p.launch_date, p.supplier_id FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating <= 3.8)
