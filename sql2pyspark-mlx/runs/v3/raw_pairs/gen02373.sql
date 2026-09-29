select dept_name, location from departments where budget <= 3253000 union all select dept_name, location from departments where location is not null
