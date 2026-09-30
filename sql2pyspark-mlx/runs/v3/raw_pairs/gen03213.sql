select
    location,
    case when budget >= 3251000 then 'high' when budget >= 1074000 then 'mid' else 'low' end as budget_bucket,
    sum(budget) as total_budget,
    count(*) as cnt
from departments
group by location, case when budget >= 3251000 then 'high' when budget >= 1074000 then 'mid' else 'low' end
having cnt < 4
