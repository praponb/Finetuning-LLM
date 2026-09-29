select d.id, d.dept_name from departments d where d.location in ('New York', 'Chiang Mai') and exists (select 1 from employees e where e.dept_id = d.id and e.level in ('L4', 'L5'))
