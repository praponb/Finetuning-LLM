with top_employees as (
    select
        id,
        manager_id,
        salary,
        row_number() over (partition by manager_id order by salary, id) as rn
    from employees
)
select id, manager_id, salary
from top_employees
where rn = 1
