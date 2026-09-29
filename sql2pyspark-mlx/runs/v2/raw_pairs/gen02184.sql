select id, hire_date, length(job_title) as job_title_length, upper(job_title) as job_title_upper from employees where manager_id in (2, 30) and job_title not in ('Director', 'Intern', 'Analyst');
