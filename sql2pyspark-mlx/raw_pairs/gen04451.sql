with ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by location order by budget desc, id asc) as dedup_rank
        from departments
    ) x
    where dedup_rank = 1
)
select
    id,
    dept_name,
    location,
    budget,
    dedup_rank
from ranked
order by dedup_rank desc nulls last
