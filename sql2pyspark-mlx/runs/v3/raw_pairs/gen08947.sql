with ranked as (
    select
        id,
        location,
        budget,
        dept_name,
        row_number() over (partition by location order by budget desc, id) as rn
    from departments
)
select
    id,
    location,
    budget,
    dept_name
from ranked
where rn <= 2;
