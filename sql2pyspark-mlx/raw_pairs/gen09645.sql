SELECT d.dept_name, COUNT(e.id) employee_count
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
GROUP BY d.dept_name
