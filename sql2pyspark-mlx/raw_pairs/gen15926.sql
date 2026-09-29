WITH enriched AS (
    SELECT
        e.id,
        e.full_name,
        e.dept_id,
        e.manager_id,
        e.salary,
        d.salary AS employee_salary
    FROM employees e
    LEFT JOIN employees d
        ON e.manager_id = d.id
),
with_dims AS (
    SELECT
        src.id,
        src.full_name,
        src.manager_id,
        src.salary,
        src.employee_salary,
        p.email,
        p.full_name AS employee_full_name,
        p.job_title
    FROM enriched src
    LEFT JOIN employees p
        ON src.manager_id <=> p.id
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY employee_salary DESC NULLS FIRST, id) AS rank_in_group
        FROM with_dims
    ) x
    WHERE rank_in_group = 1
),
ranked AS (
    SELECT
        id,
        full_name,
        manager_id,
        salary,
        employee_full_name,
        job_title
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY rank_in_group DESC, id) = 1
),
cleaned AS (
    SELECT
        *,
        REGEXP_REPLACE(full_name, ' ', '-') AS full_name_slug,
        ROUND(salary * 1.15, 2) AS salary_adjusted
    FROM ranked
    WHERE salary > 120000
),
latest_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary_adjusted DESC, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
)
SELECT
    id,
    full_name,
    manager_id,
    salary,
    employee_full_name,
    job_title,
    full_name_slug,
    salary_adjusted,
    latest_rank
FROM latest_employees
ORDER BY manager_id
