select case when budget >= 3845000 then 'high' when budget >= 1300000 then 'mid' else 'low' end as budget_bucket, dept_name, count(*) as total_count
from departments
group by case when budget >= 3845000 then 'high' when budget >= 1300000 then 'mid' else 'low' end, dept_name
having count(*) >= 4
order by budget_bucket;
