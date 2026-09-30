select e.id, e.dept_id as employee_dept_id, m.full_name as manager_full_name, m.manager_id as manager_manager_id from employees e inner join employees m on e.manager_id = m.id;
