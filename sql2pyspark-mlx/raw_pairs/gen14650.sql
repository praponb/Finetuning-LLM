WITH ranked_inventory AS (
    SELECT
        id,
        product_id,
        last_restock,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS rnk
    FROM inventory
    WHERE quantity BETWEEN 65 AND 139
)
SELECT *
FROM ranked_inventory
WHERE rnk <= 5
