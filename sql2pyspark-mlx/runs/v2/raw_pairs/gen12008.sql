SELECT d.id, COUNT(e.id) employee_count
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE d.budget > 2590000
GROUP BY d.id
ORDER BY id, employee_count
