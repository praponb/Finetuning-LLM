WITH cleaned AS (
    SELECT
        *,
        CASE WHEN location = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok,
        ROUND(budget * 100, 2) AS budget_scaled
    FROM departments
    WHERE budget >= 3689000
)
SELECT *
FROM (
    SELECT
        location,
        dept_name,
        budget_scaled
    FROM cleaned
) src
PIVOT (
    MAX(budget_scaled)
    FOR dept_name IN ('Engineering' AS engineering, 'Sales' AS sales)
);
