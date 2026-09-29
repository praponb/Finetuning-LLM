select id, dept_name, budget
from (
    select
        id,
        dept_name,
        budget,
        row_number() over (partition by dept_name order by budget desc, id) as rnk
    from departments
) as t
where rnk <= 3
