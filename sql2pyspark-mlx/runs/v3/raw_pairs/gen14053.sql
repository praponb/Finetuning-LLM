select case when budget >= 3842000 then 'high' when budget >= 1725000 then 'mid' else 'low' end as budget_bucket, round((100 * sum(case when dept_name = 'Legal' then 1 else 0 end)) / count(*), 1) as pct_legal, avg(budget) as avg_budget
from departments
group by budget_bucket
order by pct_legal
