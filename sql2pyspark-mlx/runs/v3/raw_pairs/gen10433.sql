SELECT i.id, i.quantity, s.city
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE s.city IN ('New York', 'London', 'Singapore');
