WITH ranked AS (
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS dedup_rank
        FROM employees
    ) ranked
    WHERE dedup_rank = 1
),
ranked_employees AS (
    SELECT
        id,
        manager_id,
        hire_date,
        level
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, dept_id ORDER BY hire_date DESC NULLS LAST, id) AS row_num
        FROM ranked
    ) x
    WHERE row_num = 1
),
ranked_employees_2 AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC NULLS LAST, id) AS rn
    FROM ranked_employees
    QUALIFY rn = 1
),
joined AS (
    SELECT
        a.id,
        a.manager_id,
        a.level,
        a.rn,
        e.full_name,
        e.email
    FROM ranked_employees_2 a
    INNER JOIN employees e
        ON a.manager_id <=> e.id
    WHERE e.dept_id IN (5, 6, 39)
),
enriched AS (
    SELECT
        e.id,
        e.manager_id,
        e.level,
        e.full_name,
        e.email,
        p.email AS employee_email,
        p.job_title
    FROM joined e
    LEFT JOIN employees p
        ON e.manager_id <=> p.id
),
filtered AS (
    SELECT *, CONCAT(level, ' ', job_title) AS level_job_title
    FROM enriched
    WHERE job_title IS NOT NULL
),
with_dims AS (
    SELECT
        a.id,
        a.manager_id,
        a.full_name,
        a.email,
        a.employee_email,
        b.email AS employee_email_2
    FROM filtered a
    LEFT JOIN employees b
        ON a.manager_id = b.id
)
SELECT
    s.id,
    s.manager_id,
    s.full_name,
    s.email,
    s.employee_email,
    s.employee_email_2,
    (SELECT MIN(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS min_employee_salary
FROM with_dims s
