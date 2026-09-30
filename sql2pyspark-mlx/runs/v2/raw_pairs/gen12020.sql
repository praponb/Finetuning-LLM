select sum(case when job_title = 'Analyst' then salary else 0 end) as analyst_salary, max(salary) as max_salary, max(hire_date) as latest_hire_date from employees order by max_salary desc;
