select
    location,
    case when budget >= 3077000 then 'high' when budget >= 1266000 then 'mid' else 'low' end as budget_bucket,
    min(budget) as min_budget,
    count(*) as cnt,
    round(stddev(budget), 2) as stddev_budget
from departments
group by location, budget_bucket
order by min_budget desc
