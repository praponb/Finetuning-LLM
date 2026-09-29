select *, lower(job_title) as job_title_lower, length(email) as email_length
from employees
where job_title is not null or hire_date between '2023-07-01' and '2023-08-05';
