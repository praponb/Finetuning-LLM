WITH ranked_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) pos
    FROM inventory
)
SELECT *
FROM ranked_inventory
WHERE pos = 1
ORDER BY product_id
