select
    case when d.budget >= 3142000 then 'high' when d.budget >= 2010000 then 'mid' else 'low' end as budget_bucket,
    d.dept_name,
    count(*) as num_employees,
    concat_ws(', ', sort_array(collect_set(e.job_title))) as job_title_values
from employees e
inner join departments d on e.dept_id = d.id
group by case when d.budget >= 3142000 then 'high' when d.budget >= 2010000 then 'mid' else 'low' end, d.dept_name
order by budget_bucket asc
