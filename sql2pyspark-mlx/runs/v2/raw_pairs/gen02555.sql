select id, salary, hire_date from employees where salary = (select min(salary) from employees);
