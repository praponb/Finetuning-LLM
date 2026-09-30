select manager_id, is_remote, level, lower(email) as email_lower from employees where full_name like 'Hen%' and hire_date < '2024-07-01' order by manager_id desc;
