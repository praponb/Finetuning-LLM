SELECT p.id, p.launch_date, p.supplier_id FROM products p WHERE p.price <= 420 AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city = 'Tokyo');
