SELECT d.dept_name, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(e.job_title))) AS job_title_values, ROUND(STDDEV(e.salary), 2) AS stddev_salary
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
GROUP BY d.dept_name
ORDER BY d.dept_name DESC;
