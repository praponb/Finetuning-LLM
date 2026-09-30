select max(salary) - min(salary) as salary_range, count(*) as cnt from employees where is_remote
