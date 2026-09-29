select id, email, manager_id, initcap(trim(job_title)) as job_title_title, date_add(hire_date, 17) as hire_date_plus_17d from employees;
