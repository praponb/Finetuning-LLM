WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        SUBSTRING(location, 1, 3) AS location_prefix
    FROM departments
    WHERE budget BETWEEN 1642000 AND 2387000
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.level,
        top1.employee_hire_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.level, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        x.employee_hire_date_2,
        x.employee_level
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date_2, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date < s.employee_hire_date_2
        )
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', employee_hire_date_2) AS activity_month,
        COUNT(*) AS num_rows,
        COUNT(CASE WHEN employee_level = 'L3' THEN 1 END) AS l3_count,
        SORT_ARRAY(COLLECT_SET(location)) AS location_list,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE employee_level = 'L5') AS l5_rows
    FROM kept
    GROUP BY 1
)
SELECT
    activity_month,
    l3_count,
    location_list,
    distinct_ids,
    l5_rows
FROM totals
ORDER BY l3_count DESC NULLS FIRST, distinct_ids;
