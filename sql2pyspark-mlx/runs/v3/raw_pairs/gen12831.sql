SELECT i.quantity, p.launch_date, s.city FROM inventory i LEFT JOIN products p ON i.product_id = p.id LEFT JOIN suppliers s ON s.id = p.supplier_id WHERE p.supplier_id IN (11, 19, 25, 39)
