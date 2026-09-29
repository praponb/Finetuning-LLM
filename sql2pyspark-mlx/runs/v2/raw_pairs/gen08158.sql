select
    id,
    manager_id,
    salary,
    sum(salary) over (partition by manager_id) manager_id_sum_salary
from employees;
