select id, job_title, email, level, concat_ws(',', email, full_name) as email_full_name_key from employees where hire_date >= '2023-12-01';
