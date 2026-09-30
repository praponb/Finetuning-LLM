SELECT e.email AS employee_email, m.dept_id AS manager_dept_id
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.job_title IN ('Director', 'Intern') OR e.dept_id IN (20, 27)
ORDER BY employee_email, manager_dept_id DESC
