SELECT dept_id, SORT_ARRAY(COLLECT_SET(level)) AS level_list, SUM(salary) AS total_salary
FROM employees
WHERE is_remote = TRUE
GROUP BY 1
HAVING SUM(salary) < 432000
ORDER BY dept_id DESC
