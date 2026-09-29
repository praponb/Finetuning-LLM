select
    quarter(hire_date) as hire_quarter,
    job_title,
    max(salary) as max_salary,
    round((100 * sum(case when level = 'L4' then 1 else 0 end)) / count(*), 1) as pct_l4
from employees
where hire_date between '2023-08-01' and '2025-08-15'
group by hire_quarter, job_title
