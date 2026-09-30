SELECT
    is_remote,
    MAX(salary) AS max_salary,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values,
    COUNT(*) AS total_count
FROM employees
WHERE level IN ('L5', 'L1', 'L2')
GROUP BY is_remote
ORDER BY max_salary;
