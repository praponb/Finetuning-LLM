WITH ranked_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rnk
    FROM inventory
    WHERE store_id IN (1, 6)
)
SELECT
    id,
    product_id,
    quantity,
    last_restock
FROM ranked_inventory
WHERE rnk <= 2
ORDER BY last_restock ASC, id ASC
