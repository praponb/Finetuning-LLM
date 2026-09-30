select
    id,
    location,
    budget,
    dept_name
from (
    select
        id,
        location,
        budget,
        dept_name,
        row_number() over (partition by location order by budget desc, id) as rn
    from departments
    where dept_name = 'Finance'
) as t
where rn = 1;
