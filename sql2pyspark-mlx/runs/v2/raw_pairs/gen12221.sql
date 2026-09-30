WITH RankedInventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS row_num
    FROM inventory
)
SELECT
    id,
    product_id,
    last_restock,
    store_id
FROM RankedInventory
WHERE row_num <= 3
ORDER BY product_id DESC, last_restock;
