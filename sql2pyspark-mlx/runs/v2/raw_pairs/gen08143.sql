select
    id,
    level,
    salary,
    manager_id,
    hire_date,
    avg(salary) over (partition by level) as level_avg_salary,
    rank() over (partition by manager_id order by hire_date) as rnk
from employees
where job_title = 'Designer'
order by level asc, level_avg_salary asc;
