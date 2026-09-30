select *
from (
    select
        location,
        dept_name,
        budget
    from departments
) src
pivot (
    max(budget) as total,
    count(1) as cnt
    for dept_name in ('Sales' as sales, 'Finance' as finance, 'Engineering' as engineering)
)
