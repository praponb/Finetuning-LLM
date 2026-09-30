select e.salary, e.dept_id, e.full_name, d.location from employees e join departments d on e.dept_id = d.id where d.dept_name != 'Finance' and e.manager_id is not null
