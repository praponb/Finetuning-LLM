select d.location, d.dept_name, d.budget, e.dept_id, e.manager_id, e.salary from departments d inner join employees e on d.id = e.dept_id order by manager_id desc;
