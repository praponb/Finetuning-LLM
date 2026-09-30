SELECT job_title, designer_salary, analyst_salary
FROM (
    SELECT job_title, SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) AS designer_salary, SUM(CASE WHEN job_title = 'Analyst' THEN salary ELSE 0 END) AS analyst_salary
    FROM employees
    WHERE manager_id IN (5, 23, 31)
    GROUP BY job_title
) AS x
WHERE designer_salary >= 66
ORDER BY analyst_salary DESC
