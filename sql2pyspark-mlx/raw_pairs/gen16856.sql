with recent_departments as (
    select *
    from departments
    where budget >= 1849000 and budget >= 2231000 and location not in ('Tokyo', 'Singapore', 'New York')
),
departments_stats as (
    select
        dept_name,
        case when budget >= 3770000 then 'high' when budget >= 1167000 then 'mid' else 'low' end budget_bucket,
        count(distinct budget) unique_dept_names,
        sum(case when location = 'Singapore' then 1 else 0 end) singapore_count
    from recent_departments
    group by dept_name, case when budget >= 3770000 then 'high' when budget >= 1167000 then 'mid' else 'low' end
)
select *
from departments_stats
where singapore_count >= 24
order by 4 desc;
