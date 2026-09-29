SELECT i.quantity, p.product_name, s.city, s.supplier_name FROM inventory i LEFT OUTER JOIN products p ON p.id = i.product_id INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY quantity
