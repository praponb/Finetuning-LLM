WITH RankedInventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS rn
    FROM inventory
)
SELECT *
FROM RankedInventory
WHERE rn <= 3
ORDER BY last_restock
