select case when budget >= 4302000 then 'high' when budget >= 1762000 then 'mid' else 'low' end as budget_bucket, round(stddev(budget), 2) as stddev_budget, sum(budget) as total_budget
from departments
group by 1
order by budget_bucket desc
