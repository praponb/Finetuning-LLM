select
    id,
    dept_id,
    salary,
    level,
    rank() over (partition by dept_id order by salary) rnk,
    count(*) over (partition by level) level_count
from employees;
