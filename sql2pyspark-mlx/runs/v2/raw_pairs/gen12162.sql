SELECT s.city, COUNT(*) AS row_count, COUNT(DISTINCT i.quantity) AS unique_stores
FROM inventory i
INNER JOIN stores s ON s.id = i.store_id
GROUP BY s.city
