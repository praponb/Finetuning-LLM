select case when budget >= 3508000 then 'high' when budget >= 724000 then 'mid' else 'low' end as budget_bucket, round(stddev(budget), 2) as stddev_budget, count(budget) as budget_count
from departments
group by case when budget >= 3508000 then 'high' when budget >= 724000 then 'mid' else 'low' end
order by stddev_budget desc, budget_count desc;
