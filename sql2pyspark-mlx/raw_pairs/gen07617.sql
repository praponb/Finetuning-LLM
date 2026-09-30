SELECT d.id, d.location, COUNT(e.id) AS employee_count
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE d.budget BETWEEN 2799000 AND 3977000
GROUP BY d.id, d.location
HAVING employee_count > 2;
