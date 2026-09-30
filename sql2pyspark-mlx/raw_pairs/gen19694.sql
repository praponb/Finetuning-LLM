WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN budget >= 4406000 THEN 'High' WHEN budget >= 1972000 THEN 'Medium' ELSE 'Low' END AS budget_band
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IS DISTINCT FROM 'New York'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_band,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(dept_name, 'OTHER') AS resolved_dept_name
    FROM filtered
    WHERE location IS NOT NULL
      AND location IS DISTINCT FROM 'London'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id IN (6, 8, 26)
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) = 1
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_salary,
        latest.level
    FROM latest s
    JOIN LATERAL (
            SELECT c.salary AS employee_salary, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    dept_name,
    budget,
    budget_band,
    resolved_dept_name
FROM with_latest
