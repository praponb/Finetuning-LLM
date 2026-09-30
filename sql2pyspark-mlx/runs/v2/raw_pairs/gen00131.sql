select * from (select level, job_title, salary from employees)
pivot (count(salary) for job_title in ('Engineer', 'Analyst'));
