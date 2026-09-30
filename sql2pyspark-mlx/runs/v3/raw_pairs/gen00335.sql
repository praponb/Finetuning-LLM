SELECT *, TRIM(job_title) AS clean_job_title, CEIL(salary) AS salary_whole FROM employees WHERE dept_id = 3;
