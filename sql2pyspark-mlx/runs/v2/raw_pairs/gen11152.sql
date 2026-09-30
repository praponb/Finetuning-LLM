SELECT
    hire_quarter,
    level,
    total_count,
    l1_count
FROM (
    SELECT
        QUARTER(hire_date) AS hire_quarter,
        level,
        COUNT(*) AS total_count,
        SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END) AS l1_count
    FROM employees
    WHERE full_name IS NOT NULL
    GROUP BY QUARTER(hire_date), level
) AS x
WHERE total_count > 7
ORDER BY l1_count DESC;
