select hire_date from employees where lower(email) like '%enr%' union all select hire_date from employees where lower(full_name) like '%rac%'
