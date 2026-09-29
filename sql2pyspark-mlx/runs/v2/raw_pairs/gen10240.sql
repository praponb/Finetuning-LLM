select *
from (
    select
        dept_name,
        case when budget >= 3296000 then 'high' when budget >= 645000 then 'mid' else 'low' end as budget_bucket,
        round((100 * sum(case when location = 'London' then 1 else 0 end)) / count(*), 1) as pct_london,
        count(*) as cnt
    from departments
    where dept_name in ('Finance', 'Engineering')
    group by dept_name, case when budget >= 3296000 then 'high' when budget >= 645000 then 'mid' else 'low' end
) as sub
where cnt < 6
order by cnt desc
