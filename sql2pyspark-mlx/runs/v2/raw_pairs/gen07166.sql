SELECT i.id, i.store_id, p.launch_date, s.supplier_name FROM inventory i INNER JOIN products p ON p.id = i.product_id RIGHT OUTER JOIN suppliers s ON s.id = p.supplier_id
