SELECT
    carrier,
    status,
    CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END AS shipped_cohort,
    AVG(cost) AS avg_cost,
    COUNT(DISTINCT cost) AS unique_orders
FROM shipments
GROUP BY 1, 2, 3
ORDER BY shipped_cohort, carrier
