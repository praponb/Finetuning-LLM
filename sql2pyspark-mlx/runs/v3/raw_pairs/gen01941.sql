WITH filtered_inventory AS (
    SELECT *, DATE_TRUNC('month', last_restock) AS last_restock_period
    FROM inventory
    WHERE store_id IN (1, 2, 3) OR store_id IN (2, 4, 6)
),
Agg AS (
    SELECT CASE WHEN YEAR(last_restock_period) < 2024 THEN 'before_2024' WHEN YEAR(last_restock_period) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_period_cohort, COUNT(DISTINCT quantity) AS unique_products
    FROM filtered_inventory
    GROUP BY CASE WHEN YEAR(last_restock_period) < 2024 THEN 'before_2024' WHEN YEAR(last_restock_period) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT last_restock_period_cohort, unique_products
FROM Agg
ORDER BY last_restock_period_cohort
