select d.id, d.dept_name
from departments d
where d.dept_name in ('HR', 'Engineering') and not exists (select 1 from employees e where e.dept_id = d.id and lower(trim(e.email)) = 'grace.kim@gmail.com');
