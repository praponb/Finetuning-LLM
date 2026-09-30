SELECT CASE WHEN budget >= 3986000 THEN 'high' WHEN budget >= 1175000 THEN 'mid' ELSE 'low' END budget_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) dept_name_values, ROUND(AVG(budget), 2) avg_budget
FROM departments
WHERE location IS NULL AND budget IS NOT NULL
GROUP BY budget_bucket
ORDER BY dept_name_values;
