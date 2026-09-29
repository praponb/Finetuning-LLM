WITH valid AS (
    SELECT *, COALESCE(budget, 0) budget_filled
    FROM departments
    WHERE dept_name NOT IN ('Marketing', 'Finance')
),
filtered AS (
    SELECT
        *,
        budget_filled * budget budget_filled_times_budget,
        INITCAP(TRIM(location)) location_title
    FROM valid
    WHERE budget BETWEEN 1893000 AND 2667000
),
with_last AS (
    SELECT s.*, latest.salary
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY salary DESC, id ASC) AS rn
    FROM with_last
    QUALIFY rn = 1
),
base AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_filled,
        budget_filled_times_budget,
        location_title,
        rn,
        budget_filled * budget budget_filled_times_budget_2,
        COALESCE(location, 'N/A') location_filled
    FROM latest
    WHERE dept_name IS DISTINCT FROM 'Finance'
)
SELECT
    dept_name,
    CASE WHEN budget >= 3197000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    SUM(CASE WHEN location = 'Singapore' THEN budget_filled_times_budget ELSE 0 END) AS singapore_budget_filled_times_budget,
    PERCENTILE_APPROX(budget_filled_times_budget_2, 0.5) AS budget_filled_times_budget_2_p50,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM base
GROUP BY dept_name, CASE WHEN budget >= 3197000 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY budget_tier DESC
