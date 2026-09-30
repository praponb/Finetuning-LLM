select d.budget, e.full_name, e.manager_id from departments d left outer join employees e on d.id = e.dept_id;
