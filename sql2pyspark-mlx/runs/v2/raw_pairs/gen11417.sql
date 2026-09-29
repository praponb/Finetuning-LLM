select id, hire_date, is_remote, full_name from employees where hire_date = (select max(hire_date) from employees where dept_id in (5, 34));
