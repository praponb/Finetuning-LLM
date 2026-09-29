select id, is_remote, lower(job_title) job_title_lower from employees where manager_id is not null;
