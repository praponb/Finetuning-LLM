SELECT
    e.id,
    e.level,
    e.salary,
    d.budget,
    d.dept_name
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
WHERE d.location = 'Singapore' AND e.manager_id IS NOT NULL;
