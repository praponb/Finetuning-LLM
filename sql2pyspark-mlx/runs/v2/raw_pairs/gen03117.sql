with src as (
    select *, cast(budget as int) budget_int
    from departments
    where dept_name in ('Legal', 'HR')
),
location_summary as (
    select location, case when budget >= 3333000 then 'high' when budget >= 601000 then 'mid' else 'low' end budget_bucket, count(case when dept_name = 'Engineering' then 1 end) engineering_count
    from src
    group by location, case when budget >= 3333000 then 'high' when budget >= 601000 then 'mid' else 'low' end
)
select location, budget_bucket, engineering_count
from location_summary
order by engineering_count desc, location, budget_bucket
limit 20;
