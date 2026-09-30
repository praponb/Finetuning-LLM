select
    id,
    level,
    manager_id,
    dept_id
from employees
where month(hire_date) in (4, 9, 11) or is_remote = false
