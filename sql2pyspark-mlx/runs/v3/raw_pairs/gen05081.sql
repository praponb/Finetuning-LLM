select
    d.id,
    d.location,
    count(e.id) as employee_count,
    count(distinct e.salary) as unique_depts,
    concat_ws(', ', sort_array(collect_set(e.job_title))) as job_title_values
from departments d
inner join employees e on d.id = e.dept_id
group by d.id, d.location
order by employee_count desc
