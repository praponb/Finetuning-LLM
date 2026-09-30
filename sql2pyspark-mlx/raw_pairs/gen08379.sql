select
    id,
    dept_id,
    cast(is_remote as int) as is_remote_flag,
    cast(is_remote as int) as is_remote_flag_2
from employees
where (manager_id is null and salary <= 131000) or is_remote
