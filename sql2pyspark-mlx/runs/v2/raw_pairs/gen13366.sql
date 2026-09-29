select is_remote, salary, dept_id, initcap(trim(full_name)) as full_name_title from employees where length(full_name) < 7;
