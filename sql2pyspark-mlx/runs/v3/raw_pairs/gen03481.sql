SELECT
    store_id,
    COUNT(*) AS cnt,
    MIN(quantity) AS min_quantity,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
WHERE quantity BETWEEN 202 AND 222
GROUP BY 1
HAVING AVG(quantity) < 160;
