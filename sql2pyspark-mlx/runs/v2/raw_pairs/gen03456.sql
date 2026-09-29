SELECT d.id, d.location
FROM departments d
WHERE d.id IN (SELECT dept_id FROM employees WHERE hire_date <= '2024-07-19')
ORDER BY d.location;
