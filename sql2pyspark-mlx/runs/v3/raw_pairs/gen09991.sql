SELECT e.salary, d.budget
FROM employees e
LEFT JOIN departments d ON d.id = e.dept_id
WHERE d.location = 'Berlin';
