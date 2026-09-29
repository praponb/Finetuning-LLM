SELECT p.id, p.brand, p.stock_qty FROM products p WHERE p.launch_date <= '2025-08-01' AND p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('New York', 'Bangkok', 'London'));
