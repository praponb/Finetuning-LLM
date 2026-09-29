SELECT s.id, s.city
FROM suppliers s
WHERE s.city IN ('Bangkok', 'Chiang Mai', 'New York') AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)
ORDER BY id DESC, city;
