select
    id,
    job_title,
    salary,
    sum(salary) over (partition by job_title) as job_title_sum_salary
from employees
where salary >= 122000
order by salary desc
