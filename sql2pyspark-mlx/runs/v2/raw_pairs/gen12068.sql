SELECT
    s.id,
    s.store_name,
    COUNT(i.id) inventory_count,
    COUNT(DISTINCT i.quantity) unique_products
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
WHERE s.opened_date <= '2024-02-01'
GROUP BY s.id, s.store_name
ORDER BY inventory_count, id, store_name
LIMIT 10;
