select d.budget, d.location, d.dept_name, e.manager_id, e.is_remote from departments d left join employees e on e.dept_id = d.id
