WITH enriched AS (
    -- Attach employees attributes
    SELECT
        src.id,
        src.dept_id,
        src.manager_id,
        src.job_title,
        src.hire_date,
        src.level,
        d.job_title AS employee_job_title,
        d.level AS employee_level
    FROM employees src
    LEFT JOIN employees d
        ON src.manager_id <=> d.id
)
SELECT
    s.id,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.hire_date,
    s.level,
    s.employee_job_title,
    s.employee_level,
    (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = s.dept_id) AS avg_employee_salary
FROM enriched s
ORDER BY avg_employee_salary DESC
