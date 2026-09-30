select id, full_name, hire_date from employees where is_remote = true union all select id, full_name, hire_date from employees where manager_id is not null order by id;
