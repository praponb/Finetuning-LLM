SELECT i.id, i.quantity, p.product_name, s.city FROM inventory i LEFT JOIN products p ON i.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY id DESC;
