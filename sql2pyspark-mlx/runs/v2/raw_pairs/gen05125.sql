select id, salary, initcap(trim(job_title)) job_title_title
from employees
where full_name is not null and is_remote = true
order by job_title_title, 1 desc
