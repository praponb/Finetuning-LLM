SELECT dept_name, pct_finance
FROM (
    SELECT dept_name, ROUND((100 * SUM(CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_finance
    FROM departments
    GROUP BY dept_name
) AS t
WHERE pct_finance < 60
ORDER BY 1
LIMIT 10
