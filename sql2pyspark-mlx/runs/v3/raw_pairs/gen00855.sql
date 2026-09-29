select is_remote, min(hire_date) as min_hire_date from employees where is_remote and hire_date < '2023-04-27' group by 1 order by min_hire_date, is_remote limit 20;
