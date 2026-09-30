SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
FROM employees
GROUP BY 1
HAVING MAX(salary) >= 97000
ORDER BY level_values;
