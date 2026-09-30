select
    id,
    dept_id,
    is_remote,
    upper(full_name) as full_name_upper
from employees
where email is not null
