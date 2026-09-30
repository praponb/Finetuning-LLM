select
    id,
    salary,
    full_name,
    hire_date,
    length(job_title) as job_title_length,
    concat_ws('-', full_name, level) as full_name_level_key
from employees
where manager_id in (14, 17, 30, 38)
order by job_title_length desc, id asc
limit 5
