SELECT
    product_id,
    store_id,
    MAX(quantity) max_quantity,
    AVG(quantity) avg_quantity,
    MAX(quantity) - MIN(quantity) quantity_range
FROM inventory
WHERE product_id IN (1, 4, 9)
GROUP BY product_id, store_id
HAVING AVG(quantity) > 130
