SELECT *
FROM (
    SELECT CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort, COUNT(DISTINCT quantity) AS unique_stores, SUM(quantity) AS total_quantity
    FROM inventory
    WHERE last_restock >= '2025-02-01'
    GROUP BY CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END
) AS x
WHERE total_quantity > 500
ORDER BY last_restock_cohort DESC
