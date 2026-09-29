select
    id,
    full_name,
    job_title,
    regexp_extract(email, '@(.+)$', 1) as email_domain
from employees
where hire_date < '2023-08-01'
order by job_title desc, full_name, id
limit 20;
