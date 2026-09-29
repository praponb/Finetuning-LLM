WITH kept AS (
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary > 91000
        )
),
cleaned AS (
    SELECT
        *,
        CASE WHEN location IN ('Singapore', 'Bangkok') THEN 'Priority' ELSE 'Other' END AS location_group,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled
    FROM kept
    WHERE budget IS NOT NULL
),
kept_keys AS (
    SELECT id, location
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id, location
    FROM departments
    WHERE location = 'Berlin'
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id AND s.location = k.location
),
totals AS (
    SELECT
        dept_name,
        CASE WHEN budget >= 3803000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
        COUNT_IF(location = 'Bangkok') AS bangkok_events,
        COUNT_IF(location = 'Chiang Mai') AS chiang_mai_events,
        COUNT(DISTINCT location) AS distinct_locations,
        PERCENTILE_APPROX(budget, 0.5) AS budget_p50
    FROM reconciled
    GROUP BY dept_name, budget_tier
)
SELECT
    dept_name,
    budget_tier,
    chiang_mai_events,
    distinct_locations,
    budget_p50
FROM totals;
