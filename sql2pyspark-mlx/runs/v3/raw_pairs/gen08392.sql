select
    case when budget >= 4067000 then 'high' when budget >= 1147000 then 'mid' else 'low' end as budget_bucket,
    sum(budget) as total_budget,
    count(*) as cnt,
    dense_rank() over (order by sum(budget) desc) as rnk
from departments
group by case when budget >= 4067000 then 'high' when budget >= 1147000 then 'mid' else 'low' end
order by total_budget desc;
