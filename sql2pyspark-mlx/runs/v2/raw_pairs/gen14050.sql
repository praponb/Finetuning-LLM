select e.id, e.dept_id, e.full_name, d.dept_name, d.location, d.budget from employees e inner join departments d on e.dept_id = d.id
