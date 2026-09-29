select *
from (
    select
        location,
        dept_name,
        budget
    from departments
) src
pivot (
    avg(budget) as amount,
    count(1) as cnt
    for dept_name in ('Engineering', 'Marketing', 'HR')
)
