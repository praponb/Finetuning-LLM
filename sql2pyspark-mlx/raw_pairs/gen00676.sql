select *
from (
    select *, row_number() over (partition by dept_name order by budget, id) as row_num
    from departments
) as r
where row_num <= 3
