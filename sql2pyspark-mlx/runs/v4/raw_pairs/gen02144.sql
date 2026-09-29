SELECT d.id, COUNT(e.id) employee_count, SUM(e.salary) total_salary
FROM departments d
INNER JOIN employees e ON d.id = e.dept_id
WHERE d.budget > 1977000
GROUP BY d.id
ORDER BY total_salary DESC;
