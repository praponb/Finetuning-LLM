WITH enriched AS (
    SELECT
        a.*,
        d.location,
        d.budget
    FROM employees a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
joined AS (
    SELECT
        a.*,
        e.hire_date AS employee_hire_date,
        e.job_title AS employee_job_title,
        e.email AS employee_email
    FROM enriched a
    INNER JOIN employees e
        ON a.manager_id = e.id
),
joined_employees AS (
    SELECT
        src.*,
        d.email AS employee_email_2,
        d.salary AS employee_salary,
        d.hire_date AS employee_hire_date_2
    FROM joined src
    LEFT JOIN employees d
        ON src.manager_id <=> d.id
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date ASC, id ASC) AS row_num
    FROM joined_employees
    QUALIFY row_num = 1
)
SELECT *
FROM (
    SELECT
        id,
        employee_job_title,
        budget,
        salary
    FROM latest
) src
UNPIVOT (
    value FOR kpi IN (budget AS BUDGET_VAL, salary AS SALARY_VAL)
)
