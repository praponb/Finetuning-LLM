select dept_name, department_count
from (
    select dept_name, count(*) department_count
    from departments
    where budget <= 1720000
    group by dept_name
) sub
where department_count >= 7;
