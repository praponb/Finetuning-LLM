select case when budget >= 4391000 then 'high' when budget >= 1950000 then 'mid' else 'low' end as budget_bucket, round(stddev(budget), 2) as stddev_budget
from departments
where budget < 3123000
group by budget_bucket
