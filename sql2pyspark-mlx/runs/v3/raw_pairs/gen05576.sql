WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS pos
    FROM inventory
    WHERE store_id IN (1, 3, 4, 5)
)
SELECT *
FROM ranked
WHERE pos <= 2
ORDER BY product_id DESC
