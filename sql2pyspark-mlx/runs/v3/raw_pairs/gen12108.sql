SELECT d.id, d.dept_name, COUNT(e.id) AS employee_count
FROM departments d
JOIN employees e ON e.dept_id = d.id
WHERE e.level IN ('L3', 'L4')
GROUP BY d.id, d.dept_name
HAVING COUNT(e.id) >= 2
ORDER BY dept_name
