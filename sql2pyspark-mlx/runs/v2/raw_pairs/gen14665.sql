select min(salary) as min_salary, max(salary) as max_salary from employees where is_remote and hire_date <= '2024-10-01' and email like '%@gmail.com' order by max_salary desc, min_salary desc;
