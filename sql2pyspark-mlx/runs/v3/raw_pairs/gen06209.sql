WITH ranked_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS rn
    FROM inventory
)
SELECT id, store_id, quantity
FROM ranked_inventory
WHERE rn <= 2
ORDER BY store_id DESC;
