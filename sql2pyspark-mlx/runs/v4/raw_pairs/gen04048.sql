SELECT p.id, p.category, s.rating
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id;
