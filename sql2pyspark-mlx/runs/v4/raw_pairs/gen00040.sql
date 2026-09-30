select year(hire_date) as hire_year, max(hire_date) as last_hire_date from employees where is_remote and full_name like 'Noa%' group by year(hire_date) order by last_hire_date;
