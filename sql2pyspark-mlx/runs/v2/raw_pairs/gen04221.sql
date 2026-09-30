SELECT i.id, i.product_id, p.product_name, s.rating, s.supplier_name FROM inventory i RIGHT OUTER JOIN products p ON i.product_id = p.id JOIN suppliers s ON p.supplier_id = s.id
