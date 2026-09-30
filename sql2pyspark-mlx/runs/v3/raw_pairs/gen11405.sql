select id, level, job_title, manager_id from employees where length(full_name) < 7 and email is not null
