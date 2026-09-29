select job_title, full_name, email
from employees
where full_name is not null or dept_id = 34
order by full_name, job_title;
