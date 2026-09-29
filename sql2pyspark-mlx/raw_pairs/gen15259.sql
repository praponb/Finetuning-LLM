select id, salary, job_title, floor(salary) as salary_whole, datediff(current_date(), hire_date) / 365 as years_since_hire_date from employees order by salary desc, job_title desc;
