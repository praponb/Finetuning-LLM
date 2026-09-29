SELECT
    e.id,
    e.level,
    e.full_name,
    e.dept_id,
    d.budget
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id
ORDER BY budget;
