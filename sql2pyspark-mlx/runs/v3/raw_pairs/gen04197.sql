select
    id,
    manager_id,
    hire_date,
    salary,
    round(avg(salary) over (partition by manager_id order by hire_date, id rows between 6 preceding and current row), 2) as salary_moving_avg_7,
    dense_rank() over (order by salary desc) as salary_dense_rank
from employees
