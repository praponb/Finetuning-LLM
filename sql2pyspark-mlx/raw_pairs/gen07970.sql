SELECT s.id, s.country
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE supplier_id IS NOT NULL)
