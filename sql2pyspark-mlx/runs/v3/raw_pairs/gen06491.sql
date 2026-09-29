WITH filtered_products AS (
    SELECT *
    FROM products
    WHERE cost < 480
),
products_stats AS (
    SELECT CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END launch_cohort, SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END) stark_count, COUNT(cost) cost_count
    FROM filtered_products
    GROUP BY CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT launch_cohort, stark_count, cost_count
FROM products_stats
ORDER BY stark_count;
