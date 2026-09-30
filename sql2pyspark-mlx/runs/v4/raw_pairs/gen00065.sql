with top_departments as (
    select
        id,
        location,
        budget,
        row_number() over (partition by location order by budget, id) as pos
    from departments
)
select id, location, budget
from top_departments
where pos <= 1
order by 1
