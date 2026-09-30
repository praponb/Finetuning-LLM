SELECT s.id, s.country, s.rating
FROM suppliers s
WHERE s.supplier_name NOT LIKE '%Tra%' AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.cost >= 680);
