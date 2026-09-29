SELECT d.dept_name, COUNT(*) AS total_count, SORT_ARRAY(COLLECT_SET(e.job_title)) AS job_title_list
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id
GROUP BY d.dept_name
HAVING COUNT(*) >= 3
ORDER BY d.dept_name DESC, total_count ASC
