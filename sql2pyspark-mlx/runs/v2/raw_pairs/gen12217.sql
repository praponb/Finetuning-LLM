SELECT
    e.level,
    e.hire_date,
    e.full_name,
    d.dept_name,
    d.location,
    d.budget
FROM employees e
JOIN departments d ON d.id = e.dept_id
