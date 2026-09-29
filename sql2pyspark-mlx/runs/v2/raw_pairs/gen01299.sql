SELECT
    e.id,
    e.hire_date,
    e.job_title,
    e.dept_id
FROM employees e
WHERE e.dept_id IN (9, 25) AND e.dept_id IN (SELECT id FROM departments WHERE dept_name <> 'Legal')
ORDER BY hire_date
