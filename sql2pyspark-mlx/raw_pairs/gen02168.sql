SELECT emp.salary AS employee_salary, mgr.is_remote AS manager_is_remote FROM employees emp LEFT JOIN employees mgr ON emp.manager_id = mgr.id WHERE mgr.job_title IN ('Engineer', 'Designer');
