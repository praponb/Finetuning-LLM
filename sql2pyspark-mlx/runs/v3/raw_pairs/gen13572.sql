WITH recent_inventory AS (
    SELECT *, ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
    FROM inventory
    WHERE quantity <= 138
),
inventory_stats AS (
    SELECT
        CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
        store_id,
        AVG(quantity) AS avg_quantity,
        COUNT(DISTINCT quantity) AS unique_products
    FROM recent_inventory
    GROUP BY CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END, store_id
)
SELECT
    last_restock_cohort,
    store_id,
    avg_quantity,
    unique_products
FROM inventory_stats
ORDER BY avg_quantity DESC
