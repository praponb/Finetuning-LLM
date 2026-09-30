SELECT
    e.id,
    e.hire_date,
    e.salary,
    e.manager_id
FROM employees e
WHERE e.job_title != 'Engineer' AND e.dept_id IN (SELECT id FROM departments WHERE location IN ('Berlin', 'New York'))
