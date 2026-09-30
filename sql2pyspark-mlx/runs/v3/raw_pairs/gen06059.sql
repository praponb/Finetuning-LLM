select
    case when budget >= 3485000 then 'high' when budget >= 1017000 then 'mid' else 'low' end as budget_bucket,
    dept_name,
    case when dept_name in ('Sales', 'Engineering') then 'core' else 'other' end as dept_name_group,
    sum(budget) as total_budget
from departments
where (location = 'Singapore' and location in ('Berlin', 'Bangkok')) or budget is not null
group by 1, 2, 3;
