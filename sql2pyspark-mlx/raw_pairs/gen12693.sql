select d.location, d.budget, d.dept_name, e.manager_id from departments d left join employees e on e.dept_id = d.id where d.dept_name != 'HR' and d.budget is null;
