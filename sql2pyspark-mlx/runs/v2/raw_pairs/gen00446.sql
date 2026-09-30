SELECT
    dept_name,
    SUM(budget) AS total_budget,
    COUNT(DISTINCT budget) AS unique_dept_names,
    SUM(CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END) AS hr_count
FROM departments
WHERE dept_name IN ('HR', 'Engineering')
GROUP BY dept_name
ORDER BY hr_count, dept_name
LIMIT 20;
