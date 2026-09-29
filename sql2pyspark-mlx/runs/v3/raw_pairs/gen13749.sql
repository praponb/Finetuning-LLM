WITH recent_employees AS (
    SELECT *
    FROM employees
    WHERE salary <= 78000
),
summary AS (
    SELECT CASE WHEN salary >= 162000 THEN 'high' WHEN salary >= 69000 THEN 'mid' ELSE 'low' END salary_bucket, MAX(salary) max_salary, SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) manager_count
    FROM recent_employees
    GROUP BY CASE WHEN salary >= 162000 THEN 'high' WHEN salary >= 69000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE max_salary < 128000
ORDER BY manager_count DESC;
