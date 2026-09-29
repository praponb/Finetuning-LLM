WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS row_num
    FROM inventory
)
SELECT id, store_id, quantity
FROM ranked
WHERE row_num <= 5
ORDER BY quantity DESC, id DESC;
