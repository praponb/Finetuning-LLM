SELECT *
FROM (
    SELECT product_id, MIN(last_restock) AS min_last_restock, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY product_id
) AS x
WHERE quantity_range >= 52
ORDER BY quantity_range DESC, product_id
LIMIT 50;
