WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        is_remote,
        level,
        SUBSTRING(job_title, 1, 3) AS job_title_prefix
    FROM employees
    WHERE job_title IS NOT NULL
      AND salary <= 92000
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        e.id,
        e.full_name,
        e.email,
        e.dept_id,
        e.level,
        e.job_title_prefix,
        d.budget,
        d.dept_name,
        d.location
    FROM base e
    LEFT JOIN departments d
        ON e.dept_id = d.id
),
joined AS (
    -- Attach departments attributes
    SELECT e.*, b.dept_name AS department_dept_name
    FROM with_dims e
    LEFT JOIN departments b
        ON e.dept_id = b.id
    WHERE b.budget BETWEEN 1972000 AND 3425000
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        dept_id,
        department_dept_name,
        MIN(budget) OVER w_part AS last7_min_budget,
        COUNT(budget) OVER w_part AS last7_count_budget,
        SUM(budget) OVER w_part AS last7_sum_budget,
        MAX(budget) OVER w_part AS last7_max_budget
    FROM joined
    WINDOW w_part AS (PARTITION BY level ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Join a dimension and aggregate
SELECT
    dim.dept_name,
    a.department_dept_name,
    COUNT(*) AS cnt,
    ROUND(SUM(a.last7_min_budget), 2) AS total_last7_min_budget,
    GROUPING_ID() AS gid
FROM with_windows a
JOIN departments dim ON a.dept_id = dim.id
GROUP BY GROUPING SETS ((dim.dept_name, a.department_dept_name), (dim.dept_name))
