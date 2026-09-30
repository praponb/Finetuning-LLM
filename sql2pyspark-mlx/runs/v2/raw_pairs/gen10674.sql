SELECT d.dept_name, d.location, SORT_ARRAY(COLLECT_SET(e.job_title)) AS job_title_list FROM employees e FULL OUTER JOIN departments d ON e.dept_id = d.id GROUP BY d.dept_name, d.location;
