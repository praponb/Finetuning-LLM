select hire_date from employees where hire_date is not null union all select launch_date as hire_date from products where launch_date is not null order by hire_date
