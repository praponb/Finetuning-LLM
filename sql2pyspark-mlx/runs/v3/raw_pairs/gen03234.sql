SELECT emp.id, emp.dept_id AS employee_dept_id, emp.salary AS employee_salary, mgr.email AS manager_email FROM employees emp INNER JOIN employees mgr ON emp.manager_id = mgr.id ORDER BY mgr.email;
