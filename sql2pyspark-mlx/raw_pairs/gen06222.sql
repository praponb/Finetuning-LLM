WITH remaining_keys AS (
    SELECT id, level
    FROM employees
    EXCEPT ALL
    SELECT id, level
    FROM employees
    WHERE is_remote = FALSE
),
verified AS (
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.level = k.level
),
enriched AS (
    SELECT a.*, e.hire_date AS employee_hire_date
    FROM verified a
    LEFT JOIN employees e
        ON a.manager_id <=> e.id
    WHERE e.full_name IS NULL
),
joined AS (
    SELECT
        src.id,
        src.full_name,
        src.email,
        src.dept_id,
        src.manager_id,
        src.level,
        src.employee_hire_date,
        d.dept_name,
        d.budget,
        d.location
    FROM enriched src
    INNER JOIN departments d
        ON src.dept_id = d.id
),
filtered AS (
    SELECT
        *,
        LOWER(level) AS level_lower,
        TRIM(full_name) AS clean_full_name
    FROM joined
    WHERE level IS NOT NULL
      AND location != 'Tokyo'
)
SELECT
    DATE_TRUNC('MONTH', employee_hire_date) AS activity_month,
    dept_id,
    SUM(CASE WHEN dept_name = 'Marketing' THEN budget ELSE 0 END) AS marketing_budget,
    COUNT_IF(dept_name = 'HR') AS hr_events,
    COUNT(*) FILTER (WHERE level = 'L3') AS l3_rows,
    SUM(budget) AS total_budget,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names
FROM filtered
GROUP BY DATE_TRUNC('MONTH', employee_hire_date), dept_id;
