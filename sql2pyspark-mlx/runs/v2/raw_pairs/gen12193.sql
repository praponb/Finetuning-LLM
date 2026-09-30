select
    id,
    manager_id,
    hire_date,
    salary,
    lead(salary, 1) over (partition by manager_id order by hire_date, id) as next_salary
from employees
