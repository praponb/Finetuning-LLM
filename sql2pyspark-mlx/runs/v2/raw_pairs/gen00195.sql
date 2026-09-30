select
    id,
    manager_id,
    job_title,
    regexp_extract(email, '@(.+)$', 1) as email_domain
from employees
where dept_id in (3, 24, 33, 40) and dept_id in (25, 26, 32);
