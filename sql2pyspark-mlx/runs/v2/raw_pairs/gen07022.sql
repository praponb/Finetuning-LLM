SELECT emp.level AS employee_level, mgr.salary AS manager_salary FROM employees emp LEFT JOIN employees mgr ON emp.manager_id = mgr.id ORDER BY employee_level DESC;
