select
    id,
    salary,
    is_remote,
    manager_id,
    trim(job_title) as clean_job_title
from employees;
