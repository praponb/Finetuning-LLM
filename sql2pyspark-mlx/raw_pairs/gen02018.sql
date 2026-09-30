WITH src AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (1, 2, 5, 6)
),
summary AS (
    SELECT CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort, MAX(quantity) - MIN(quantity) AS quantity_range, MIN(last_restock) AS first_last_restock
    FROM src
    GROUP BY CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT last_restock_cohort, first_last_restock
FROM summary;
