select hire_date, job_title from employees where lower(trim(full_name)) = 'bob smith' order by job_title;
