WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS rnk
    FROM inventory
)
SELECT id, store_id, last_restock
FROM ranked
WHERE rnk <= 3
ORDER BY id
