select
    id,
    hire_date,
    email,
    upper(full_name) as full_name_upper,
    length(job_title) as job_title_length
from employees
where level is null and hire_date between '2023-10-06' and '2023-10-20'
order by email desc;
