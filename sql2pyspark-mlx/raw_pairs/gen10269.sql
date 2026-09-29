WITH filtered AS (
    SELECT *, CAST(salary AS INT) AS salary_int
    FROM employees
    WHERE hire_date BETWEEN '2023-11-14' AND '2025-01-25'
),
scored AS (
    SELECT
        *,
        LAG(salary_int) OVER (PARTITION BY level, job_title ORDER BY hire_date, id) AS prev_salary_int,
        LEAD(salary_int) OVER (PARTITION BY level, job_title ORDER BY hire_date, id) AS next_salary_int,
        LAG(salary) OVER (PARTITION BY level, job_title ORDER BY hire_date, id) AS prev_salary,
        LEAD(salary) OVER (PARTITION BY level, job_title ORDER BY hire_date, id) AS next_salary,
        ROW_NUMBER() OVER (PARTITION BY level, job_title ORDER BY hire_date, id) AS seq_no
    FROM filtered
),
enriched AS (
    SELECT
        a.*,
        d.full_name AS employee_full_name,
        d.email AS employee_email
    FROM scored a
    LEFT JOIN employees d
        ON a.manager_id = d.id
    WHERE d.level NOT IN ('L1', 'L2')
),
with_dims AS (
    SELECT
        a.id,
        a.full_name,
        a.email,
        a.dept_id,
        a.manager_id,
        a.hire_date,
        a.prev_salary_int,
        a.next_salary_int,
        a.prev_salary,
        a.next_salary,
        a.seq_no,
        a.employee_email,
        e.full_name AS employee_full_name_2,
        e.job_title AS employee_job_title,
        e.email AS employee_email_2
    FROM enriched a
    LEFT JOIN employees e
        ON a.manager_id = e.id
),
filtered_employees AS (
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        hire_date,
        prev_salary_int,
        next_salary_int,
        next_salary,
        seq_no,
        employee_email,
        employee_full_name_2,
        employee_job_title,
        employee_email_2,
        UPPER(employee_job_title) AS employee_job_title_upper
    FROM with_dims
    WHERE hire_date > DATE '2023-04-09'
      AND manager_id IS NOT NULL
)
SELECT
    s.id,
    s.email,
    s.dept_id,
    s.hire_date,
    s.prev_salary_int,
    s.next_salary,
    s.seq_no,
    s.employee_email,
    s.employee_full_name_2,
    (SELECT MIN(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS min_employee_salary
FROM filtered_employees s
ORDER BY next_salary DESC NULLS FIRST;
