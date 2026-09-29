SELECT
    s.id,
    s.region,
    COUNT(i.id) AS inventory_count,
    MAX(i.quantity) AS max_quantity,
    COUNT(DISTINCT i.quantity) AS unique_stores
FROM stores s
INNER JOIN inventory i ON s.id = i.store_id
WHERE YEAR(i.last_restock) = 2025
GROUP BY s.id, s.region
ORDER BY s.region DESC, id
LIMIT 20
