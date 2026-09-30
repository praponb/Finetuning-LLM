WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS row_num
    FROM inventory
)
SELECT *
FROM ranked
WHERE row_num = 1
ORDER BY row_num
