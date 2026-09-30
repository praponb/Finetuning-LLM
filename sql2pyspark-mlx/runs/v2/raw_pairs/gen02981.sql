select case when budget >= 3117000 then 'high' when budget >= 660000 then 'mid' else 'low' end as budget_bucket, sum(case when location = 'Tokyo' then 1 else 0 end) as tokyo_count
from departments
where budget is not null and dept_name in ('Finance', 'Legal') and budget is not null
group by budget_bucket;
