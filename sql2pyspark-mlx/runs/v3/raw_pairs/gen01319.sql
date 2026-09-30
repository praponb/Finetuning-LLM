select *
from employees
where month(hire_date) in (1, 4, 12) or job_title <> 'Analyst'
