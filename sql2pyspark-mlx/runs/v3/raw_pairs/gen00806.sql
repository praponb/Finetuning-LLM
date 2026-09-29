SELECT s.country, s.supplier_name, p.launch_date, p.supplier_id FROM suppliers s INNER JOIN products p ON p.supplier_id = s.id;
