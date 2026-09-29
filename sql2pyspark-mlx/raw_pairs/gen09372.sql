with ranked as (
    select *, rank() over (partition by dept_name order by budget desc) as pos
    from departments
)
select
    id,
    dept_name,
    budget,
    location
from ranked
where pos = 1
