SELECT
    s.id,
    s.city,
    COUNT(i.id) AS inventory_count,
    SUM(i.quantity) AS total_quantity
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
WHERE i.last_restock > '2024-02-29'
GROUP BY s.id, s.city
ORDER BY inventory_count;
