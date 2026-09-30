select
    id,
    level,
    date_trunc('week', hire_date) hire_date_period,
    cast(salary as int) salary_int
from employees;
