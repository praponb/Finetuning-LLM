select
    d.location,
    case when d.budget >= 4183000 then 'high' when d.budget >= 708000 then 'mid' else 'low' end budget_bucket,
    count(*) cnt,
    count(distinct e.salary) unique_managers
from employees e
left join departments d on d.id = e.dept_id
group by d.location, case when d.budget >= 4183000 then 'high' when d.budget >= 708000 then 'mid' else 'low' end
having count(*) >= 5
order by location;
