select d.id, d.budget, d.location, d.dept_name from departments d where d.budget >= 1717000 and d.id in (select dept_id from employees where salary < 111000)
