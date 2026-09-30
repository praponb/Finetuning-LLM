SELECT *
FROM (
    SELECT CASE WHEN salary >= 163000 THEN 'high' WHEN salary >= 43000 THEN 'mid' ELSE 'low' END AS salary_bucket, MIN(hire_date) AS min_hire_date, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    GROUP BY CASE WHEN salary >= 163000 THEN 'high' WHEN salary >= 43000 THEN 'mid' ELSE 'low' END
) AS sub
WHERE salary_range > 33
ORDER BY salary_bucket
