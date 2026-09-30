select e.id, e.manager_id from employees e where e.dept_id not in (select id from departments where location in ('Chiang Mai', 'Berlin', 'Singapore'))
