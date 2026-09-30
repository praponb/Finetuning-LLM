SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, ROUND(STDDEV(salary), 2) AS stddev_salary
FROM employees
WHERE level = 'L4'
GROUP BY 1
ORDER BY hire_cohort
