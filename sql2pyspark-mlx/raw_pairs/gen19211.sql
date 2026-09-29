with eligible as (
    select s.*
    from departments s
    where exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.manager_id = 36
        )
)
select
    id,
    dept_name,
    location,
    budget
from eligible
order by dept_name desc;
