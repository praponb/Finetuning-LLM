select
    id,
    manager_id,
    hire_date,
    salary,
    lag(salary, 2) over (partition by manager_id order by hire_date, id) as prev_salary
from employees
where hire_date > '2025-07-01'
order by prev_salary desc;
