select
    d.id,
    d.location,
    count(e.id) as employee_count,
    round((100 * sum(case when e.job_title = 'Analyst' then 1 else 0 end)) / count(*), 1) as pct_analyst
from departments d
left join employees e on e.dept_id = d.id
group by d.id, d.location
order by pct_analyst desc, employee_count desc, id, location
limit 20;
