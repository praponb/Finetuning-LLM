SELECT d.location, COUNT(e.id) AS employee_count
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
GROUP BY d.location
HAVING employee_count >= 4
ORDER BY d.location ASC
