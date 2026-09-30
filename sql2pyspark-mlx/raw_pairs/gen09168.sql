with ranked_departments as (
    select *, row_number() over (partition by dept_name order by budget desc, id) as rn
    from departments
)
select
    id,
    dept_name,
    budget,
    location
from ranked_departments
where rn <= 3;
