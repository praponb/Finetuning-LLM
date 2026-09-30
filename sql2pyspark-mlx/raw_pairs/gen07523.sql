with recent_departments as (
    select *
    from departments
    where dept_name = 'Finance'
),
summary as (
    select case when budget >= 4100000 then 'high' when budget >= 676000 then 'mid' else 'low' end as budget_bucket, count(*) as row_count
    from recent_departments
    group by case when budget >= 4100000 then 'high' when budget >= 676000 then 'mid' else 'low' end
)
select *
from summary;
