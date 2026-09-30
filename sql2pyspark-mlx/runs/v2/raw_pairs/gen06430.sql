SELECT s.id, s.city
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE brand IN ('Stark', 'Acme'))
