select *
from (
    select
        dept_id,
        level,
        salary
    from employees
) src
pivot (
    max(salary)
    for level in ('L1', 'L4', 'L5')
)
