select budget_bucket, dept_name, berlin_count
from (
    select case when budget >= 4436000 then 'high' when budget >= 872000 then 'mid' else 'low' end as budget_bucket, dept_name, count(case when location = 'Berlin' then 1 end) as berlin_count
    from departments
    group by case when budget >= 4436000 then 'high' when budget >= 872000 then 'mid' else 'low' end, dept_name
) as s
where berlin_count > 9;
