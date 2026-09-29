SELECT
    store_id,
    COUNT(quantity) AS quantity_count,
    MIN(quantity) AS min_quantity,
    SUM(quantity) AS total_quantity
FROM inventory
GROUP BY store_id
HAVING total_quantity >= 1440
