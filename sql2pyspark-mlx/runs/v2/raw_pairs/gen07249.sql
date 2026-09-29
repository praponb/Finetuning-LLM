SELECT level, first_hire_date, cnt
FROM (
    SELECT level, MIN(hire_date) AS first_hire_date, COUNT(*) AS cnt
    FROM employees
    GROUP BY level
) AS x
WHERE cnt < 6
