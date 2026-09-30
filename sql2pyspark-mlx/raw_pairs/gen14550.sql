SELECT
    s.id,
    s.region,
    COUNT(i.id) inventory_count,
    COUNT(i.quantity) quantity_count,
    MAX(i.quantity) max_quantity
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
GROUP BY s.id, s.region
