select
    id,
    level,
    salary,
    max(salary) over (partition by level) level_max_salary
from employees
where hire_date <= '2025-04-17'
order by level_max_salary, level
