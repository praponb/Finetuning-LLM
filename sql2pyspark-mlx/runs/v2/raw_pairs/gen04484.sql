select id, manager_id, salary
from (
    select
        id,
        manager_id,
        salary,
        dense_rank() over (partition by manager_id order by salary) as rnk
    from employees
) as r
where rnk <= 1
