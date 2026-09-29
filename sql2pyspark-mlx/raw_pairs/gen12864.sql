select emp.is_remote employee_is_remote, mgr.email manager_email from employees emp inner join employees mgr on emp.manager_id = mgr.id where mgr.salary <= 73000;
