select id, hire_date, concat_ws('-', full_name, email) full_name_email_key from employees where not (dept_id = 7 and (not is_remote or dept_id in (6, 8, 14, 36)))
