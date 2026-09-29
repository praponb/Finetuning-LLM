SELECT *
FROM (
    SELECT CASE WHEN salary >= 132000 THEN 'high' WHEN salary >= 84000 THEN 'mid' ELSE 'low' END AS salary_bucket, ROUND((100 * SUM(CASE WHEN level = 'L3' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l3, MIN(hire_date) AS first_hire_date
    FROM employees
    WHERE LOWER(email) LIKE '%rac%'
    GROUP BY CASE WHEN salary >= 132000 THEN 'high' WHEN salary >= 84000 THEN 'mid' ELSE 'low' END
) AS x
WHERE pct_l3 > 61
ORDER BY pct_l3, first_hire_date;
