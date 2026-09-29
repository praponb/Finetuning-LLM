SELECT d.id, COUNT(e.id) AS employee_count, MIN(e.salary) AS min_salary
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
WHERE e.manager_id IS NOT NULL
GROUP BY d.id
ORDER BY employee_count DESC
