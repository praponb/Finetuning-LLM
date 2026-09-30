select salary, is_remote, concat(job_title, ' - ', level) as job_title_level from employees where email like '%@gmail.com' order by job_title_level, salary
