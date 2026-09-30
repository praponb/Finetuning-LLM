select d.budget, e.is_remote from departments d left join employees e on d.id = e.dept_id
