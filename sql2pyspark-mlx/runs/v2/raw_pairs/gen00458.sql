SELECT s.id, s.rating
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE category = 'Home')
ORDER BY s.id ASC;
