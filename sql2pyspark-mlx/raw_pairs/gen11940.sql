SELECT s.supplier_name, p.price FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id;
