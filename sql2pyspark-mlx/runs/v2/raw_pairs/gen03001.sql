SELECT
    e.id,
    e.manager_id,
    e.hire_date,
    d.dept_name
FROM employees e
LEFT JOIN departments d ON d.id = e.dept_id
WHERE d.location = 'Berlin';
