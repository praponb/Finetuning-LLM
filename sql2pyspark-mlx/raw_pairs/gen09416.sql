select
    id,
    manager_id,
    salary,
    round(salary / sum(salary) over (partition by manager_id), 3) as salary_share
from employees
