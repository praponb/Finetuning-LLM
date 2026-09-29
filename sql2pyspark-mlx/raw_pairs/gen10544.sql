SELECT job_title, SUM(CASE WHEN level = 'L1' THEN salary ELSE 0 END) l1_salary
FROM employees
WHERE hire_date BETWEEN '2023-11-01' AND '2024-03-27'
GROUP BY job_title
ORDER BY l1_salary, job_title
LIMIT 10
