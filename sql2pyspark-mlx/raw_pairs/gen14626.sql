SELECT job_title, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
FROM employees
WHERE manager_id IS NOT NULL OR email LIKE '%@gmail.com'
GROUP BY job_title
