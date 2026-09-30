select
    id,
    dept_id,
    salary,
    sum(salary) over (partition by dept_id) as dept_id_sum_salary
from employees
where dept_id in (2, 4, 29, 30);
