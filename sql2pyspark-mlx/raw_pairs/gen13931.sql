WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(dept_name, 1, 2) AS dept_name_prefix,
        CONCAT_WS(',', location, dept_name) AS location_dept_name_key
    FROM departments
    WHERE location = 'Berlin'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_dept_name_key,
        LAG(budget) OVER w_grp AS prev_budget,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM cleaned
    WHERE budget <= 2160000
    WINDOW w_grp AS (PARTITION BY dept_name ORDER BY budget, id)
),
matched_keys AS (
    -- Set difference on keys
    SELECT location
    FROM scored
    EXCEPT DISTINCT
    SELECT location
    FROM departments
    WHERE dept_name = 'Engineering'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN matched_keys k ON s.location = k.location
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.email IS NOT NULL
        )
)
-- Grouped metrics
SELECT
    CASE WHEN prev_budget >= 3287000 THEN 'LARGE' ELSE 'SMALL' END AS prev_budget_tier,
    CASE WHEN budget >= 3092000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    ROUND(AVG(prev_budget), 2) AS avg_prev_budget,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE dept_name = 'Finance') AS finance_rows,
    GROUPING(CASE WHEN prev_budget >= 3287000 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
FROM eligible
GROUP BY ROLLUP (CASE WHEN prev_budget >= 3287000 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN budget >= 3092000 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY finance_rows;
