SELECT s.city, s.supplier_name, p.product_name FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id;
