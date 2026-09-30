select case when budget >= 3956000 then 'high' when budget >= 1146000 then 'mid' else 'low' end as budget_bucket, dept_name, round((100 * sum(case when location = 'Bangkok' then 1 else 0 end)) / count(*), 1) as pct_bangkok
from departments
where budget <= 3240000
group by case when budget >= 3956000 then 'high' when budget >= 1146000 then 'mid' else 'low' end, dept_name
order by pct_bangkok;
