select dept_id, coalesce(job_title, 'unassigned') as job_title_filled from employees;
