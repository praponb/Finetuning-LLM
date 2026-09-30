SELECT *
FROM (
    SELECT CASE WHEN salary >= 157000 THEN 'high' WHEN salary >= 75000 THEN 'mid' ELSE 'low' END AS salary_bucket, dept_id, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    GROUP BY CASE WHEN salary >= 157000 THEN 'high' WHEN salary >= 75000 THEN 'mid' ELSE 'low' END, dept_id
) AS sub
WHERE salary_range > 47
ORDER BY salary_bucket, dept_id DESC
