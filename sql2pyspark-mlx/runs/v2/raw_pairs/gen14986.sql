select
    d.dept_name,
    count(e.id) employee_count,
    sum(case when e.job_title = 'Designer' then 1 else 0 end) designer_count,
    sort_array(collect_set(e.level)) level_list
from departments d
left join employees e on d.id = e.dept_id
group by d.dept_name
having count(e.id) >= 3
