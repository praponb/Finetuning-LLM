select hire_date, full_name from employees where dept_id = 40 union select hire_date, full_name from employees where level in ('L1', 'L3', 'L5')
