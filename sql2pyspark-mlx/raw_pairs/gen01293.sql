select id, salary, email, hire_date from employees where salary > (select max(salary) from employees where hire_date <= '2024-09-01')
