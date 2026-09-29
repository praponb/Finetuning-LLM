select e.is_remote, d.budget, d.location from employees e right outer join departments d on e.dept_id = d.id order by d.location, budget desc
