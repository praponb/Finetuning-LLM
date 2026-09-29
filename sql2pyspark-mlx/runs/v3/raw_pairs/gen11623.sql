SELECT SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list, COUNT(CASE WHEN job_title = 'Manager' THEN 1 END) AS manager_count
FROM employees
WHERE is_remote = TRUE;
