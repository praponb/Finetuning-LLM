SELECT *
FROM (
    SELECT dept_name, COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END) AS engineering_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values
    FROM departments
    WHERE budget BETWEEN 1908000 AND 3438000
    GROUP BY dept_name
) AS x
WHERE engineering_count < 5
ORDER BY dept_name_values DESC
