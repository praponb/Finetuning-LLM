select id, job_title, date_trunc('MONTH', hire_date) as hire_date_period from employees where job_title is not null and dept_id = 14 order by job_title desc, hire_date_period desc, id limit 10
