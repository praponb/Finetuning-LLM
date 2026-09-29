WITH numbered AS (
    SELECT
        id,
        product_id,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS rnk
    FROM inventory
    WHERE store_id IN (2, 4, 5)
)
SELECT id, product_id, last_restock
FROM numbered
WHERE rnk = 1
ORDER BY id DESC, product_id DESC;
