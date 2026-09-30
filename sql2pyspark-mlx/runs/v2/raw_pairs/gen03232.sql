select id, hire_date from employees where hire_date > (select max(hire_date) from employees where length(full_name) < 9) order by 1 desc, hire_date
