SELECT location, sales_count
FROM (
    SELECT location, SUM(CASE WHEN dept_name = 'Sales' THEN 1 ELSE 0 END) AS sales_count
    FROM departments
    GROUP BY location
) AS sub
WHERE sales_count >= 62
