WITH ranked_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS rnk
    FROM inventory
)
SELECT
    id,
    store_id,
    last_restock,
    quantity
FROM ranked_inventory
WHERE rnk <= 3;
