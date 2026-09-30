SELECT emp.id, emp.hire_date AS employee_hire_date, mgr.is_remote AS manager_is_remote FROM employees AS emp LEFT JOIN employees AS mgr ON emp.manager_id = mgr.id;
