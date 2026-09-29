WITH filtered_inventory AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (1, 6) OR quantity = 139
),
last_restock_year_summary AS (
    SELECT YEAR(last_restock) AS last_restock_year, MAX(quantity) AS max_quantity
    FROM filtered_inventory
    GROUP BY YEAR(last_restock)
)
SELECT *
FROM last_restock_year_summary
ORDER BY 1;
