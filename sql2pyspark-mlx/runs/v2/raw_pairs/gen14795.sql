SELECT job_title, COUNT(CASE WHEN level = 'L5' THEN 1 END) AS l5_count
FROM employees
GROUP BY job_title;
