SELECT s.id, s.country
FROM suppliers s
WHERE s.city <> 'London' AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.category NOT IN ('Electronics', 'Books'))
ORDER BY country DESC, id
LIMIT 1
