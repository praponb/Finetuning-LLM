select *
from (
    select dept_name, min(budget) as min_budget
    from departments
    where location = 'Tokyo'
    group by dept_name
) as s
where min_budget > 3061000
order by dept_name;
