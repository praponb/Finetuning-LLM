SELECT
    carrier,
    shipped_cohort,
    total_cost,
    avg_cost
FROM (
    SELECT
        carrier,
        CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END AS shipped_cohort,
        SUM(cost) AS total_cost,
        ROUND(AVG(cost), 2) AS avg_cost
    FROM shipments
    GROUP BY carrier, CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS t
WHERE total_cost >= 440;
