select d.id, d.location, d.dept_name, d.budget from departments d where d.id in (select dept_id from employees where hire_date >= '2024-01-23')
