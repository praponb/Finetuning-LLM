select e.full_name, d.budget from employees e left join departments d on e.dept_id = d.id;
