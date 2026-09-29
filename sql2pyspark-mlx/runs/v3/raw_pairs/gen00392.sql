select hire_date from employees where length(email) >= 5 union select hire_date from employees where salary >= 64000 order by hire_date
