select case when budget >= 4018000 then 'high' when budget >= 926000 then 'mid' else 'low' end as budget_bucket, min(budget) as min_budget, sum(case when dept_name = 'HR' then budget else 0 end) as hr_budget
from departments
group by 1
order by min_budget desc, hr_budget
