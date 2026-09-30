SELECT emp.hire_date AS employee_hire_date, mgr.dept_id AS manager_dept_id, mgr.email AS manager_email
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.dept_id IN (22, 33, 40)
ORDER BY 1
