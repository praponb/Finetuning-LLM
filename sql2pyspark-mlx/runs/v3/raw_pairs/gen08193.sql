SELECT i.store_id, i.quantity, p.launch_date, s.supplier_name FROM inventory i INNER JOIN products p ON i.product_id = p.id RIGHT OUTER JOIN suppliers s ON p.supplier_id = s.id;
