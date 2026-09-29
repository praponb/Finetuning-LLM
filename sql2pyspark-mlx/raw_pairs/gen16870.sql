SELECT job_title, SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) AS designer_salary
FROM employees
WHERE full_name IS NOT NULL AND level = 'L3'
GROUP BY job_title
HAVING designer_salary > 79;
