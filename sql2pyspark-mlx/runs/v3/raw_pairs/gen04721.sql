select departments.location, departments.budget, departments.dept_name, employees.level from departments inner join employees on departments.id = employees.dept_id order by dept_name;
