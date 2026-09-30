select case when budget >= 3475000 then 'high' when budget >= 2014000 then 'mid' else 'low' end as budget_bucket, sum(budget) as total_budget, round((100 * sum(case when dept_name = 'Engineering' then 1 else 0 end)) / count(*), 1) as pct_engineering
from departments
where dept_name in ('Sales', 'Legal', 'HR')
group by budget_bucket;
