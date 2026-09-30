SELECT
    DATE_FORMAT(hire_date, 'yyyy-MM') hire_month,
    CASE WHEN salary >= 156000 THEN 'high' WHEN salary >= 52000 THEN 'mid' ELSE 'low' END salary_bucket,
    dept_id,
    SUM(CASE WHEN job_title = 'Director' THEN salary ELSE 0 END) director_salary,
    MAX(hire_date) last_hire_date
FROM employees
GROUP BY DATE_FORMAT(hire_date, 'yyyy-MM'), CASE WHEN salary >= 156000 THEN 'high' WHEN salary >= 52000 THEN 'mid' ELSE 'low' END, dept_id
HAVING SUM(CASE WHEN job_title = 'Director' THEN salary ELSE 0 END) > 55
ORDER BY salary_bucket;
