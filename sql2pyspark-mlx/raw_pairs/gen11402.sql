select
    id,
    level,
    dept_id,
    hire_date,
    row_number() over (partition by level, dept_id order by hire_date desc, id) as row_num
from employees
order by level desc
