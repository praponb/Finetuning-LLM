SELECT
    e.salary,
    e.job_title,
    e.level,
    d.dept_name
FROM employees e
INNER JOIN departments d ON d.id = e.dept_id
WHERE d.dept_name IN ('HR', 'Sales', 'Marketing')
