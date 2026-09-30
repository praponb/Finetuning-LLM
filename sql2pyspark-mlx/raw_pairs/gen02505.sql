select max(hire_date) as latest_hire_date, sum(case when job_title = 'Analyst' then 1 else 0 end) as analyst_count from employees order by 1;
