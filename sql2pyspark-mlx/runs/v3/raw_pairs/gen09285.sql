select
    id,
    dept_name,
    budget,
    location
from (
    select *, dense_rank() over (partition by dept_name order by budget) as rn
    from departments
) as r
where rn <= 2;
