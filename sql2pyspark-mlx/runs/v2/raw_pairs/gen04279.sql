select d.id, d.dept_name from departments d where not exists (select 1 from employees e where e.dept_id = d.id and e.salary >= 139000);
