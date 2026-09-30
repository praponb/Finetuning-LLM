SELECT
    s.id,
    s.full_name,
    s.dept_id,
    s.job_title,
    s.salary,
    s.hire_date,
    d.budget,
    d.dept_name
FROM employees s
INNER JOIN departments d
    ON s.dept_id = d.id
WHERE d.location = 'Bangkok'
