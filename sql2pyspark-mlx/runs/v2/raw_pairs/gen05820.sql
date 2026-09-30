SELECT id, supplier_name FROM suppliers WHERE LOWER(supplier_name) LIKE '%oky%' UNION SELECT id, supplier_name FROM suppliers WHERE city NOT IN ('Bangkok', 'New York');
