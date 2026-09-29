with Ranked as (
    select
        id,
        location,
        budget,
        row_number() over (partition by location order by budget desc, id) pos
    from departments
)
select id, location, budget
from Ranked
where pos <= 3
order by 2
