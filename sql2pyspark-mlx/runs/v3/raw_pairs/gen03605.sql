WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock) AS pos
    FROM inventory
    WHERE store_id IN (1, 3, 5, 6)
)
SELECT id, store_id, last_restock
FROM Ranked
WHERE pos <= 3
