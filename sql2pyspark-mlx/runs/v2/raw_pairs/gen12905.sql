select e.level employee_level, m.manager_id manager_manager_id from employees e left join employees m on e.manager_id = m.id where m.email like '%@startup.io';
