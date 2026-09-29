SELECT CASE WHEN salary >= 152000 THEN 'high' WHEN salary >= 70000 THEN 'mid' ELSE 'low' END salary_bucket, job_title, COUNT(CASE WHEN level = 'L3' THEN 1 END) l3_count
FROM employees
WHERE manager_id IS NOT NULL AND salary > 102000
GROUP BY CUBE(CASE WHEN salary >= 152000 THEN 'high' WHEN salary >= 70000 THEN 'mid' ELSE 'low' END, job_title);
