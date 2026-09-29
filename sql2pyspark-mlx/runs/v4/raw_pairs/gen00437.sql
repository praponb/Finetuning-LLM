WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN budget >= 4357000 THEN 'High' WHEN budget >= 2002000 THEN 'Medium' ELSE 'Low' END AS budget_band,
        CAST(budget AS INT) AS budget_int,
        COALESCE(dept_name, 'OTHER') AS dept_name_clean
    FROM departments
    WHERE location IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN dept_name IN ('Legal', 'Sales') THEN 'Tier 1' ELSE 'Other' END AS dept_name_group,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM ranked
    WHERE location IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_band,
        budget_int,
        dept_name_clean,
        dept_name_group,
        resolved_dept_name,
        AVG(budget_int) OVER (PARTITION BY dept_name ORDER BY budget_int, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_int_avg_5,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS seq_no
    FROM valid
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title IN ('Manager', 'Engineer')
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget_int
    FROM kept
) src
PIVOT (
    AVG(budget_int) AS value_sum
    FOR location IN ('Chiang Mai' AS chiang_mai, 'Singapore' AS singapore, 'Berlin' AS berlin)
);
