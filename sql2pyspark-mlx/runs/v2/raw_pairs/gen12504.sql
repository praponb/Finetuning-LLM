WITH filtered_inventory AS (
    SELECT *
    FROM inventory
    WHERE last_restock BETWEEN '2025-02-01' AND '2025-08-05' AND last_restock < '2024-09-01'
),
InventoryStats AS (
    SELECT DATE_FORMAT(last_restock, 'yyyy-MM') last_restock_month, MAX(quantity) max_quantity, MIN(quantity) min_quantity
    FROM filtered_inventory
    GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM')
)
SELECT *
FROM InventoryStats;
