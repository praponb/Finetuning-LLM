SELECT
    e.id,
    e.full_name,
    e.level,
    d.dept_name,
    d.budget,
    d.location
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
WHERE UPPER(e.email) LIKE '%ARO%';
