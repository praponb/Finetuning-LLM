select
    e.dept_id,
    e.is_remote,
    e.full_name,
    d.dept_name
from employees e
inner join departments d on e.dept_id = d.id
where d.dept_name in ('Legal', 'Sales', 'Engineering')
order by dept_id desc;
