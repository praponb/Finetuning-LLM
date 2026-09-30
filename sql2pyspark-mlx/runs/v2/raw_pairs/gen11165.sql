SELECT s.region, COUNT(i.id) AS inventory_count, COUNT(DISTINCT i.quantity) AS unique_products
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id
GROUP BY s.region
HAVING COUNT(DISTINCT i.quantity) >= 2;
