SELECT d.id, d.location, d.dept_name
FROM departments d
WHERE d.location IN ('New York', 'Tokyo') AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND YEAR(e.hire_date) >= 2024);
