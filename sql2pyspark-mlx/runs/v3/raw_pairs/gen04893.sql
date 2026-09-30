select case when budget >= 4169000 then 'high' when budget >= 2017000 then 'mid' else 'low' end as budget_bucket, sum(budget) as total_budget, count(distinct budget) as unique_locations
from departments
where location = 'Bangkok'
group by case when budget >= 4169000 then 'high' when budget >= 2017000 then 'mid' else 'low' end;
