WITH recent_employees AS (
    SELECT *
    FROM employees
    WHERE LOWER(TRIM(email)) = 'carol@company.co'
),
agg AS (
    SELECT CASE WHEN salary >= 140000 THEN 'high' WHEN salary >= 49000 THEN 'mid' ELSE 'low' END AS salary_bucket, SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END) AS l4_count
    FROM recent_employees
    GROUP BY CASE WHEN salary >= 140000 THEN 'high' WHEN salary >= 49000 THEN 'mid' ELSE 'low' END
)
SELECT salary_bucket, l4_count
FROM agg
WHERE l4_count >= 58
ORDER BY salary_bucket DESC
LIMIT 3
