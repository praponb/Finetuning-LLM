select
    id,
    manager_id,
    hire_date,
    full_name
from (
    select
        id,
        manager_id,
        hire_date,
        full_name,
        dense_rank() over (partition by manager_id order by hire_date desc) as rn
    from employees
) as r
where rn = 1
order by full_name desc
