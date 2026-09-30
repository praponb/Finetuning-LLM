WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS pos
    FROM inventory
)
SELECT *
FROM ranked
WHERE pos = 1
ORDER BY quantity DESC;
