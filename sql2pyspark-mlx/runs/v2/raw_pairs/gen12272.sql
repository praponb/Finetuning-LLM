SELECT emp.dept_id AS employee_dept_id, mgr.level AS manager_level
FROM employees AS emp
LEFT JOIN employees AS mgr ON emp.manager_id = mgr.id
ORDER BY mgr.level, emp.dept_id DESC
