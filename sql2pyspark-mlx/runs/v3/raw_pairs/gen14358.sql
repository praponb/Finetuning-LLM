select
    d.location,
    e.job_title,
    e.hire_date,
    e.dept_id,
    case when d.budget >= 4045000 then 'High' when d.budget >= 1468000 then 'Medium' else 'Low' end as budget_tier
from departments d
left join employees e on e.dept_id = d.id;
