SELECT
    e.id,
    e.hire_date,
    e.dept_id,
    e.level,
    d.budget
FROM employees e
JOIN departments d ON e.dept_id = d.id
