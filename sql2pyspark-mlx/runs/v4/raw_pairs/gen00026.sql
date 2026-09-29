with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by dept_name, location order by budget desc, id) as row_num
        from departments
    ) ranked
    where row_num = 1
)
select
    id,
    dept_name,
    location,
    budget,
    row_num
from ranked;
