WITH filtered_inventory AS (
    SELECT *
    FROM inventory
    WHERE quantity IS NOT NULL
),
inventory_stats AS (
    SELECT MONTH(last_restock) AS last_restock_month, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM filtered_inventory
    GROUP BY MONTH(last_restock)
)
SELECT *
FROM inventory_stats
WHERE avg_quantity > 32
ORDER BY avg_quantity;
