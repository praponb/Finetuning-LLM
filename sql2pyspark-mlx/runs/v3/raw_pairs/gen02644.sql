select id, salary, hire_date from employees where salary > (select max(salary) from employees where is_remote = true)
