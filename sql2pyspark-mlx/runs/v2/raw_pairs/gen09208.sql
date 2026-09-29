WITH valid_inventory AS (
    SELECT *, ROUND(quantity * 100, 2) AS quantity_scaled
    FROM inventory
    WHERE product_id IN (4, 6, 9)
),
inventory_stats AS (
    SELECT product_id, MIN(last_restock) AS first_last_restock, COUNT(quantity_scaled) AS quantity_scaled_count
    FROM valid_inventory
    GROUP BY product_id
)
SELECT *
FROM inventory_stats
WHERE quantity_scaled_count < 7
ORDER BY product_id DESC
