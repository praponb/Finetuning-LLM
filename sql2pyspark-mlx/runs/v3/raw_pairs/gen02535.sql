select
    id,
    level,
    salary,
    full_name,
    last_value(full_name) over (partition by level order by salary, id rows between unbounded preceding and unbounded following) as last_full_name
from employees
where hire_date > '2024-05-01'
order by full_name asc;
