WITH RankedInventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS pos
    FROM inventory
    WHERE store_id = 1
)
SELECT id, product_id, last_restock
FROM RankedInventory
WHERE pos <= 1
ORDER BY last_restock
