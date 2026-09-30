select id, hire_date, job_title from employees where hire_date = (select min(hire_date) from employees)
