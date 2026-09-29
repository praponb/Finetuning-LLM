select e.id, e.manager_id as employee_manager_id, m.hire_date as manager_hire_date from employees e join employees m on e.manager_id = m.id where e.is_remote;
