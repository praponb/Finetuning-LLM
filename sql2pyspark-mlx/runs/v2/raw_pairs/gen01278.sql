select id, hire_date from employees where dept_id in (6, 13, 16, 33) union all select id, hire_date from employees where salary between 113000 and 127000
