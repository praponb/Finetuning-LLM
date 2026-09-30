SELECT
    employees.id,
    employees.full_name,
    employees.salary,
    employees.job_title
FROM employees
WHERE employees.salary >= 101000 AND employees.dept_id IN (SELECT id FROM departments WHERE dept_name IN ('Finance', 'Sales', 'Marketing'))
