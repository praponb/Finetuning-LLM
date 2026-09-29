select dept_id from employees where year(hire_date) >= 2023 except select dept_id from employees where email is not null
