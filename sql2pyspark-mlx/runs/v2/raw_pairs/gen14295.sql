SELECT s.id, s.rating
FROM suppliers s
WHERE s.city IS NOT NULL AND s.id IN (SELECT supplier_id FROM products)
ORDER BY id, s.rating DESC;
