SELECT s.id, s.country
FROM suppliers s
WHERE s.city IN ('Chiang Mai', 'Singapore', 'New York') AND s.id IN (SELECT supplier_id FROM products WHERE ARRAY_CONTAINS(tags, 'eco'));
