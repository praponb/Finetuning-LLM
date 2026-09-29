select
    id,
    dept_id,
    salary,
    rank() over (partition by dept_id order by salary desc) as salary_rank
from employees
where is_remote
order by salary_rank;
