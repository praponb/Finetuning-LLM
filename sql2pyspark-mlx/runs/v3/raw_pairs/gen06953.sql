SELECT e.id, e.dept_id AS employee_dept_id, m.full_name AS manager_full_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.job_title = 'Director'
ORDER BY id DESC
