select hire_date, dept_id from employees where dept_id in (2, 21, 30) union select hire_date, dept_id from employees where dept_id in (8, 12, 29, 38);
