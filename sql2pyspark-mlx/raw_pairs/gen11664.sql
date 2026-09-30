WITH ranked_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) rn
    FROM inventory
)
SELECT *
FROM ranked_inventory
WHERE rn = 1;
