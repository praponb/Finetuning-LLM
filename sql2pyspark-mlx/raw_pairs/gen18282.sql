select departments.id, departments.budget, departments.dept_name from departments where exists (select 1 from employees where employees.dept_id = departments.id) order by departments.budget
