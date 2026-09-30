SELECT s.id, s.region, COUNT(i.id) AS inventory_count
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id
GROUP BY s.id, s.region
ORDER BY inventory_count DESC
