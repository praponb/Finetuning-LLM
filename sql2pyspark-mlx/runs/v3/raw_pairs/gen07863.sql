SELECT p.id, p.product_name, p.cost FROM products p WHERE p.brand = 'Initech' AND p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('Chiang Mai', 'Singapore'))
