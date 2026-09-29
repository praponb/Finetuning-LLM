SELECT
    dept_name,
    ROUND(STDDEV(budget), 2) stddev_budget,
    SUM(CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END) marketing_count,
    COUNT(DISTINCT budget) unique_dept_names
FROM departments
GROUP BY dept_name
ORDER BY marketing_count DESC, dept_name
LIMIT 50
