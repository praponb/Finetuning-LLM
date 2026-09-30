SELECT
    id,
    store_id,
    last_restock,
    product_id
FROM (
    SELECT
        id,
        store_id,
        last_restock,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS pos
    FROM inventory
    WHERE product_id IN (6, 7)
) AS r
WHERE pos <= 1
ORDER BY product_id DESC;
