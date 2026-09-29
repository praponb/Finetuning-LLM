SELECT d.id, COUNT(e.id) AS employee_count, AVG(e.salary) AS avg_salary
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE d.dept_name IN ('Engineering', 'Legal')
GROUP BY d.id
ORDER BY avg_salary DESC
