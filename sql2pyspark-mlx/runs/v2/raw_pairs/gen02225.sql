SELECT i.product_id, p.product_name, p.supplier_id, s.supplier_name, s.city FROM inventory i LEFT JOIN products p ON i.product_id = p.id LEFT JOIN suppliers s ON s.id = p.supplier_id
