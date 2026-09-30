select d.id, d.dept_name from departments d where d.dept_name not in ('Marketing', 'Sales') and not exists (select 1 from employees e where e.dept_id = d.id);
