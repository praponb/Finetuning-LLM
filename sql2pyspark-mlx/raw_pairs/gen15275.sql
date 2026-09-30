SELECT i.id, i.last_restock, i.quantity, p.supplier_id, s.supplier_name FROM inventory i INNER JOIN products p ON i.product_id = p.id LEFT JOIN suppliers s ON p.supplier_id = s.id;
