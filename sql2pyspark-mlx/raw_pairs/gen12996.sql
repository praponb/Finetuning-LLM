SELECT i.quantity, p.product_name, s.supplier_name FROM inventory i INNER JOIN products p ON i.product_id = p.id INNER JOIN suppliers s ON s.id = p.supplier_id ORDER BY quantity DESC, s.supplier_name
