SELECT
    e.id,
    e.is_remote,
    e.full_name,
    e.hire_date,
    d.budget
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE e.manager_id = 19
