select d.id, d.location, d.dept_name from departments d where not exists (select 1 from employees e where e.dept_id = d.id and e.manager_id in (3, 7, 10, 27)) order by id desc
