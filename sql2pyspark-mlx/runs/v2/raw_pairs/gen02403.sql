select
    location,
    case when budget >= 3984000 then 'high' when budget >= 1706000 then 'mid' else 'low' end as budget_bucket,
    sum(case when location = 'Berlin' then 1 else 0 end) as berlin_count,
    sum(budget) as total_budget
from departments
group by location, case when budget >= 3984000 then 'high' when budget >= 1706000 then 'mid' else 'low' end
having avg(budget) < 3494000
