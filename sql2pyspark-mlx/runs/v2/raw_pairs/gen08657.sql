select
    id,
    hire_date,
    salary,
    manager_id
from employees
where not is_remote
order by manager_id;
