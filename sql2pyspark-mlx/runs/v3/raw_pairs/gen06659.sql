select case when budget >= 4274000 then 'high' when budget >= 2003000 then 'mid' else 'low' end budget_bucket, sum(budget) total_budget, sum(case when dept_name = 'Engineering' then 1 else 0 end) engineering_count
from departments
group by budget_bucket
having engineering_count > 77
order by total_budget desc;
