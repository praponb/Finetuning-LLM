SELECT e.id, e.dept_id, d.location
FROM employees e
FULL OUTER JOIN departments d ON e.dept_id = d.id
WHERE e.is_remote AND d.budget <= 1908000;
