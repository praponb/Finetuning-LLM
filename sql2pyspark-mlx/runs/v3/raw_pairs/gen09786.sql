SELECT d.location, COUNT(e.id) employee_count
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE e.hire_date >= '2025-01-01'
GROUP BY d.location
