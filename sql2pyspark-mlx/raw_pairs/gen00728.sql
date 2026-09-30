WITH ranked_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS row_num
    FROM inventory
)
SELECT
    id,
    product_id,
    last_restock,
    store_id
FROM ranked_inventory
WHERE row_num = 1
ORDER BY product_id, id DESC
