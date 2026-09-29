select round(avg(salary), 2) avg_salary, max(hire_date) latest_hire_date, sort_array(collect_set(job_title)) job_title_list from employees where full_name is not null and (is_remote or dept_id = 10);
