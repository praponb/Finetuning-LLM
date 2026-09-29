SELECT e.is_remote AS employee_is_remote, e.manager_id AS employee_manager_id, m.dept_id AS manager_dept_id
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE LOWER(e.email) LIKE '%iam%'
