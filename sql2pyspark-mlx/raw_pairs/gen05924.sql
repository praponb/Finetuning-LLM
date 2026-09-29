SELECT dept_name, SUM(budget) AS total_budget, SUM(CASE WHEN dept_name = 'Sales' THEN 1 ELSE 0 END) AS sales_count
FROM departments
WHERE location IN ('New York', 'Singapore') AND dept_name = 'Legal'
GROUP BY dept_name
ORDER BY total_budget DESC, dept_name
LIMIT 5;
