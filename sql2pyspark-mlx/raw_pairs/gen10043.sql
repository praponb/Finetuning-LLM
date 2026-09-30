WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(salary, 2) AS salary_rounded
    FROM employees
    WHERE hire_date > DATE '2023-05-10'
      AND level IN ('L2', 'L4', 'L5')
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.dept_id,
        a.manager_id,
        a.job_title,
        a.salary_rounded,
        p.dept_name,
        p.budget
    FROM cleaned a
    JOIN departments p
        ON a.dept_id = p.id
),
cleaned_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary_rounded,
        dept_name,
        budget,
        CONCAT_WS('-', dept_name, job_title) AS dept_name_job_title_key,
        TRIM(dept_name) AS clean_dept_name
    FROM with_dims
    WHERE dept_name IN ('Finance', 'HR')
),
base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(budget, 0) AS budget_filled
    FROM cleaned_employees
    WHERE job_title IS DISTINCT FROM 'Designer'
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.dept_id,
        a.manager_id,
        a.dept_name,
        d.salary
    FROM base a
    LEFT JOIN employees d
        ON a.manager_id = d.id
    WHERE d.dept_id IN (13, 19, 38)
),
base_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(salary, 2) AS salary_rounded
    FROM joined
    WHERE dept_name IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    dim.job_title,
    dim.level,
    COUNT(*) AS rows_total,
    ROUND(AVG(e.salary_rounded), 2) AS avg_salary_rounded,
    AVG(e.salary) AS avg_salary,
    GROUPING_ID() AS grouping_level
FROM base_employees e
JOIN employees dim ON e.manager_id = dim.id
GROUP BY GROUPING SETS ((dim.job_title, dim.level), (dim.job_title))
