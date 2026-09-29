select
    id,
    hire_date,
    salary,
    lead(salary, 2, 0) over (order by hire_date, id) as next_salary
from employees
