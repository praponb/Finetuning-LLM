with RankedEmployees as (
    select *, row_number() over (partition by level order by hire_date, id) as rn
    from employees
    where length(email) >= 9
)
select
    id,
    level,
    hire_date,
    full_name
from RankedEmployees
where rn <= 2;
