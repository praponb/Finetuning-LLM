SELECT p.id, p.supplier_id, s.rating, s.city FROM products p FULL OUTER JOIN suppliers s ON p.supplier_id = s.id ORDER BY supplier_id DESC;
