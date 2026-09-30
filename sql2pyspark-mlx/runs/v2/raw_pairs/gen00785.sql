select * from (select date_format(hire_date, 'yyyy-MM') as hire_month, job_title, salary from employees)
pivot (count(salary) for job_title in ('Analyst', 'Designer', 'Manager'))
