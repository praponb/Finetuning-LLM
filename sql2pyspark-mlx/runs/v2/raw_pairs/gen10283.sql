select d.id, d.dept_name from departments d where d.dept_name = 'Sales' and d.id in (select dept_id from employees where hire_date < '2023-12-01')
