select case when budget >= 3352000 then 'high' when budget >= 696000 then 'mid' else 'low' end as budget_bucket, max(budget) as max_budget
from departments
where location = 'Bangkok'
group by case when budget >= 3352000 then 'high' when budget >= 696000 then 'mid' else 'low' end
having max_budget >= 1228000
