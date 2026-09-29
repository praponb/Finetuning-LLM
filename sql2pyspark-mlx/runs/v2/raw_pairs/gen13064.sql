select d.dept_name, d.location, d.budget, e.is_remote, e.dept_id, e.email from departments d left join employees e on d.id = e.dept_id where d.budget > 2018000 and month(e.hire_date) in (5, 10, 11);
