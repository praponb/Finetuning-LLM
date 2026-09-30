WITH base AS (
    SELECT
        *,
        CONCAT_WS('/', location, dept_name) AS location_dept_name_key,
        CASE WHEN budget >= 4404000 THEN 'High' WHEN budget >= 1448000 THEN 'Medium' ELSE 'Low' END AS budget_tier
    FROM departments
    WHERE location IN ('Bangkok', 'Tokyo')
),
eligible AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date < '2024-10-01'
        )
),
with_last AS (
    SELECT
        s.*,
        x.level,
        x.job_title
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.level, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible_departments AS (
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.email LIKE '%@company.co'
        )
),
metrics AS (
    SELECT
        *,
        LAG(budget, 1) OVER w_grp AS prev_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        PERCENT_RANK() OVER (ORDER BY budget) AS budget_percent_rank
    FROM eligible_departments
    WHERE level IS NOT NULL
    WINDOW w_grp AS (ORDER BY budget, id)
),
kept_keys AS (
    SELECT dept_name, id
    FROM metrics
    INTERSECT ALL
    SELECT dept_name, id
    FROM departments
    WHERE location <> 'New York'
),
verified AS (
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.dept_name = k.dept_name AND s.id = k.id
)
SELECT
    id,
    dept_name,
    location,
    budget,
    location_dept_name_key,
    budget_tier,
    level,
    job_title,
    prev_budget
FROM verified
ORDER BY id DESC
