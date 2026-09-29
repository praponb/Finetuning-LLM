SELECT
    e.id,
    e.hire_date,
    e.email,
    e.dept_id
FROM employees e
WHERE YEAR(e.hire_date) >= 2024 AND e.dept_id IN (SELECT id FROM departments WHERE dept_name IN ('Legal', 'Finance', 'Engineering'))
