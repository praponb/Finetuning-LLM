select employees.id, employees.hire_date, employees.job_title from employees where employees.job_title = 'Manager' and employees.dept_id in (select id from departments where location is null)
