select
    d.id,
    d.dept_name,
    count(e.id) as employee_count,
    count(distinct e.salary) as unique_depts
from departments d
left join employees e on e.dept_id = d.id
where d.dept_name in ('HR', 'Marketing')
group by d.id, d.dept_name
order by employee_count desc, d.id desc
