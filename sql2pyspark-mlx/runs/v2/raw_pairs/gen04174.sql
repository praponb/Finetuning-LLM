select
    id,
    level,
    hire_date,
    rank() over (partition by level order by hire_date) rnk
from employees
order by hire_date, id desc;
