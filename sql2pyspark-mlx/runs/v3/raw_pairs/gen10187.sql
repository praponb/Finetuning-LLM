select id, job_title, level from employees where not is_remote union all select id, job_title, level from employees where level = 'L2'
