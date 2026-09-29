select d.location, d.budget, d.dept_name, e.hire_date, e.salary, e.level from departments d inner join employees e on d.id = e.dept_id;
