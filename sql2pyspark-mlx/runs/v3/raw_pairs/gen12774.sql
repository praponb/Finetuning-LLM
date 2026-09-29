select
    id,
    hire_date,
    salary,
    dept_id,
    lead(salary, 1) over (order by hire_date, id) as next_salary,
    lead(salary, 2) over (partition by dept_id order by hire_date, id) as next_salary_2
from employees;
