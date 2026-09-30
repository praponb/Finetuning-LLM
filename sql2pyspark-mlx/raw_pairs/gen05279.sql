SELECT
    s.id,
    s.dept_name,
    x.employee_job_title,
    x.hire_date
FROM departments s
JOIN LATERAL (
        SELECT c.job_title AS employee_job_title, c.hire_date
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.salary DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
