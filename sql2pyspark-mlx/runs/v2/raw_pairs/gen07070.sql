WITH RecentInventory AS (
    SELECT *
    FROM inventory
    WHERE quantity < 141
),
inventory_stats AS (
    SELECT
        product_id,
        store_id,
        AVG(quantity) AS avg_quantity,
        MAX(last_restock) AS latest_last_restock
    FROM RecentInventory
    GROUP BY product_id, store_id
)
SELECT product_id, store_id, avg_quantity
FROM inventory_stats
WHERE avg_quantity > 200
ORDER BY 3, product_id, store_id
LIMIT 5
