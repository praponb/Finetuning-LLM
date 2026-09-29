select id, hire_date, salary, sum(salary) over (order by hire_date, id rows between unbounded preceding and current row) as running_salary from employees
