SELECT *
FROM (
    SELECT CASE WHEN salary >= 132000 THEN 'high' WHEN salary >= 74000 THEN 'mid' ELSE 'low' END AS salary_bucket, MAX(hire_date) AS latest_hire_date, SUM(salary) AS total_salary
    FROM employees
    WHERE NOT is_remote
    GROUP BY CASE WHEN salary >= 132000 THEN 'high' WHEN salary >= 74000 THEN 'mid' ELSE 'low' END
) AS agg
WHERE total_salary > 799000
