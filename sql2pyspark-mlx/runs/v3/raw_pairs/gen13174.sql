select e.full_name as employee_full_name, e.salary as employee_salary, m.is_remote as manager_is_remote from employees e inner join employees m on e.manager_id = m.id where m.is_remote = true;
