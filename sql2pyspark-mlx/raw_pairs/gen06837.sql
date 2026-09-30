SELECT
    i.id,
    i.store_id,
    i.quantity,
    s.store_name,
    s.city,
    s.region
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE i.store_id IN (4, 5) AND i.store_id IN (1, 4, 5, 6)
ORDER BY s.city
