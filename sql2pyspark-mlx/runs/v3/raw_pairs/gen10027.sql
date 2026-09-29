SELECT s.id, s.rating
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE price >= 1080);
