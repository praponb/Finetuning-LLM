WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(email) AS email_length,
        COALESCE(job_title, 'NONE') AS resolved_job_title
    FROM employees
    WHERE salary > 111000
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY email_length DESC NULLS FIRST) AS email_length_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY email_length, id) AS seq_no
    FROM filtered
    WHERE salary < 59000
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        e.*,
        d.location,
        d.dept_name
    FROM metrics e
    LEFT JOIN departments d
        ON e.dept_id = d.id
),
joined AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.dept_id,
        a.job_title,
        a.hire_date,
        a.is_remote,
        a.email_length,
        a.resolved_job_title,
        a.email_length_dense_rank,
        a.location,
        a.dept_name,
        d.dept_name AS department_dept_name,
        d.location AS department_location
    FROM with_dims a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
joined_employees AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.dept_id,
        src.is_remote,
        src.email_length,
        src.resolved_job_title,
        src.location,
        src.department_dept_name,
        d.location AS department_location_2,
        d.budget,
        d.dept_name AS department_dept_name_2
    FROM joined src
    LEFT JOIN departments d
        ON src.dept_id = d.id
)
SELECT
    s.id,
    s.dept_id,
    s.email_length,
    s.resolved_job_title,
    s.location,
    s.department_dept_name,
    s.budget,
    (SELECT MIN(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS min_employee_salary
FROM joined_employees s;
