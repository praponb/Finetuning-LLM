SELECT p.id, p.supplier_id, s.country FROM products p LEFT JOIN suppliers s ON s.id = p.supplier_id WHERE p.brand = 'Acme' ORDER BY 3, id LIMIT 10;
