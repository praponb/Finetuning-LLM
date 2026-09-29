select * from (select level, job_title, salary from employees where job_title in ('Intern', 'Analyst', 'Designer'))
pivot (count(salary) for job_title in ('Director', 'Designer', 'Intern', 'Manager'));
