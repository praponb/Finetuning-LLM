select
    budget_bucket,
    dept_name,
    max_budget,
    tokyo_budget
from (
    select
        case when budget >= 3184000 then 'high' when budget >= 1757000 then 'mid' else 'low' end as budget_bucket,
        dept_name,
        max(budget) as max_budget,
        sum(case when location = 'Tokyo' then budget else 0 end) as tokyo_budget
    from departments
    group by case when budget >= 3184000 then 'high' when budget >= 1757000 then 'mid' else 'low' end, dept_name
) as x
where max_budget < 2417000;
