SELECT
    s.id,
    s.region,
    COUNT(i.id) AS inventory_count,
    AVG(i.quantity) AS avg_quantity,
    MIN(i.last_restock) AS first_last_restock
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
GROUP BY s.id, s.region;
