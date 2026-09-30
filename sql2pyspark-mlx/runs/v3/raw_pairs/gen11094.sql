SELECT s.id, s.rating, s.city
FROM suppliers s
WHERE s.country IN ('JP', 'DE') AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id);
