SELECT
    YEAR(hire_date) AS hire_year,
    COUNT(*) AS total_count,
    ROUND(STDDEV(salary), 2) AS stddev_salary,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
FROM employees
GROUP BY YEAR(hire_date);
