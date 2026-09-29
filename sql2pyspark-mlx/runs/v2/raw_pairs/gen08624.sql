SELECT
    product_id,
    store_id,
    COUNT(*) AS total_count,
    COUNT(quantity) AS quantity_count,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
GROUP BY product_id, store_id
HAVING total_count > 3
ORDER BY total_count DESC, quantity_count DESC;
