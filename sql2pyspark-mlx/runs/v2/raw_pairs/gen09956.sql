select id, hire_date, full_name, is_remote from employees where hire_date >= (select max(hire_date) from employees);
