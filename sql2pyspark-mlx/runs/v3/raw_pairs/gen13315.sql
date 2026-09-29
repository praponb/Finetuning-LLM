WITH ranked_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS pos
    FROM inventory
)
SELECT *
FROM ranked_inventory
WHERE pos <= 3;
