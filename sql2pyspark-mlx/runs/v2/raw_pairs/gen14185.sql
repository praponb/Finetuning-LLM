select id, hire_date, salary, salary - lag(salary, 1) over (order by hire_date, id) as salary_change from employees where is_remote order by 4 desc
