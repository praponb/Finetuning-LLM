SELECT *, DATE_TRUNC('WEEK', hire_date) hire_date_period, CAST(is_remote AS INT) is_remote_flag FROM employees WHERE job_title IN ('Manager', 'Director', 'Engineer') AND dept_id = 29
