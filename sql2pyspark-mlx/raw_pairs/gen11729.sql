select
    *,
    avg(salary) over (partition by level, dept_id order by salary, id rows between 6 preceding and 1 preceding) as salary_avg_6_prior,
    round(salary / sum(salary) over (partition by manager_id), 4) as salary_share,
    ntile(4) over (order by salary desc, id) as salary_bucket,
    row_number() over (partition by manager_id order by hire_date, id) as event_seq
from employees
where upper(trim(email)) = 'HENRY@STARTUP.IO'
