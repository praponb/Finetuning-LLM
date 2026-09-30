select job_title, round(avg(salary), 2) as avg_salary, sum(case when job_title = 'Engineer' then 1 else 0 end) as engineer_count from employees where is_remote group by job_title order by 2 desc
