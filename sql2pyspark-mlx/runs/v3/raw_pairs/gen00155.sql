SELECT id, product_id, last_restock
FROM (
    SELECT
        id,
        product_id,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS rn
    FROM inventory
) AS r
WHERE rn <= 5
