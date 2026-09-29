select
    id,
    dept_id,
    level,
    salary,
    avg(salary) over (partition by dept_id, level) as dept_id_avg_salary
from employees
where level = 'L5'
