select * from (select level, job_title, salary from employees where length(full_name) < 10)
pivot (avg(salary) for job_title in ('Director', 'Intern', 'Analyst'))
