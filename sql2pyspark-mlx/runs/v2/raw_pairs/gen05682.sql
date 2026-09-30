SELECT emp.manager_id AS employee_manager_id, mgr.dept_id AS manager_dept_id
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
ORDER BY mgr.dept_id
