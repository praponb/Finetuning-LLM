select
    e.level as employee_level,
    s.job_title as employee_job_title,
    count(*) as rows_total,
    min(s.salary) as min_salary,
    grouping_id() as gid
from employees s
left join employees e on s.manager_id = e.id
group by rollup (e.level, s.job_title)
order by employee_level asc nulls first;
