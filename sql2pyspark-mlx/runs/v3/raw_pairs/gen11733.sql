SELECT dept_id, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count
FROM employees
GROUP BY dept_id
ORDER BY dept_id DESC, l5_count DESC
LIMIT 1
