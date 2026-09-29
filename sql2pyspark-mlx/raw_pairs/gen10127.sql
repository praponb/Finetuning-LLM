select departments.location, employees.hire_date, employees.salary, employees.email from departments left join employees on departments.id = employees.dept_id order by salary desc;
