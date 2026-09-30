SELECT s.id, s.country
FROM suppliers s
WHERE s.city IN ('Bangkok', 'Tokyo', 'Singapore') AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.category = 'Home')
ORDER BY s.country DESC;
