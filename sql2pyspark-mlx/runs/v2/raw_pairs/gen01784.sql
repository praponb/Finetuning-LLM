SELECT salary_bucket, designer_count
FROM (
    SELECT CASE WHEN salary >= 156000 THEN 'high' WHEN salary >= 68000 THEN 'mid' ELSE 'low' END AS salary_bucket, COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count
    FROM employees
    WHERE YEAR(hire_date) = 2025
    GROUP BY CASE WHEN salary >= 156000 THEN 'high' WHEN salary >= 68000 THEN 'mid' ELSE 'low' END
) AS sub
WHERE designer_count > 4
ORDER BY designer_count DESC;
