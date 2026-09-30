SELECT job_title, ROUND((100 * SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_manager, MAX(salary) AS max_salary
FROM employees
WHERE hire_date BETWEEN '2024-12-28' AND '2025-07-26' AND manager_id IN (2, 16, 20, 35)
GROUP BY job_title
HAVING ROUND((100 * SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 61
