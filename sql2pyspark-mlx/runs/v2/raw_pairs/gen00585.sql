SELECT *
FROM (
    SELECT CASE WHEN salary >= 148000 THEN 'high' WHEN salary >= 42000 THEN 'mid' ELSE 'low' END AS salary_bucket, MONTH(hire_date) AS hire_month, SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END) AS l4_count
    FROM employees
    GROUP BY CASE WHEN salary >= 148000 THEN 'high' WHEN salary >= 42000 THEN 'mid' ELSE 'low' END, MONTH(hire_date)
) AS agg
WHERE l4_count >= 29
