select departments.id, departments.budget from departments where exists (select 1 from employees where employees.dept_id = departments.id)
