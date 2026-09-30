select id, is_remote, salary, upper(job_title) as job_title_upper, lower(job_title) as job_title_lower from employees where dept_id = 14 and dept_id in (16, 37) and month(hire_date) in (3, 9, 10);
