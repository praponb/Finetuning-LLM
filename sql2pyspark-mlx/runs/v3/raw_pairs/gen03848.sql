select id, salary, is_remote, manager_id, last_day(hire_date) as hire_date_month_end, cast(is_remote as int) as is_remote_flag from employees;
