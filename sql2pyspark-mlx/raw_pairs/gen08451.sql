SELECT
    e.id,
    e.salary,
    e.dept_id,
    e.email,
    d.budget
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE e.salary BETWEEN 90000 AND 109000
ORDER BY salary, e.email
