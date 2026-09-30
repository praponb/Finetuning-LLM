SELECT s.id, COUNT(i.id) AS inventory_count
FROM stores s
INNER JOIN inventory i ON s.id = i.store_id
WHERE i.store_id IN (2, 5)
GROUP BY s.id
HAVING COUNT(i.id) >= 5
