select id, job_title from employees where not (month(hire_date) in (4, 9, 12) and not is_remote) order by job_title;
