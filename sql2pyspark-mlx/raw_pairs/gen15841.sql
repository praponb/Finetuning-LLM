SELECT p.id, p.product_name, s.rating
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.supplier_name IS NOT NULL
