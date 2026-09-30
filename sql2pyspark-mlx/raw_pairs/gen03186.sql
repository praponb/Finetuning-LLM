with eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from departments s
    where exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.manager_id is null
        )
)
select id, budget
from eligible
order by budget desc;
