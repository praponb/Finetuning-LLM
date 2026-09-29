select
    id,
    manager_id,
    level,
    salary,
    row_number() over (partition by manager_id, level order by salary desc, id) row_num
from employees
