select d.id, d.budget, d.location, d.dept_name from departments d where d.location = 'Berlin' and d.id in (select dept_id from employees where email not like '%@outlook.com')
