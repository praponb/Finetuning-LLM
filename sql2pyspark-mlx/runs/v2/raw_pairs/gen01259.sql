select d.id, d.budget, d.location, d.dept_name from departments d where exists (select 1 from employees e where e.dept_id = d.id and e.level not in ('L1', 'L5'));
