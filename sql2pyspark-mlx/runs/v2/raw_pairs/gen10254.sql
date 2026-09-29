select * from (select date_format(hire_date, 'yyyy-MM') as hire_month, job_title, salary from employees)
pivot (count(salary) for job_title in ('Engineer', 'Manager'))
order by hire_month;
