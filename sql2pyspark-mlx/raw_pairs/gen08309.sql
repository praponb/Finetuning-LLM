select
    id,
    full_name,
    job_title,
    salary,
    is_remote,
    level,
    sum(salary) over (partition by manager_id order by hire_date, id rows between unbounded preceding and current row) as cumulative_salary,
    first_value(job_title, true) over (partition by job_title order by salary, id rows between unbounded preceding and current row) as first_known_job_title
from employees
