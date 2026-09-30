select id, job_title, hire_date, salary, sum(salary) over (partition by job_title order by hire_date, id) as running_salary from employees
