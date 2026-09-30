select case when budget >= 3714000 then 'high' when budget >= 1619000 then 'mid' else 'low' end as budget_bucket, sum(case when dept_name = 'Finance' then 1 else 0 end) as finance_count, sum(budget) as total_budget
from departments
group by 1;
