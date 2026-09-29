select id, dept_id, salary, concat_ws(',', level, job_title) as level_job_title_key from employees where is_remote;
