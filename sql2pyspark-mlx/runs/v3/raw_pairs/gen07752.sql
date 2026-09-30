SELECT
    manager_id,
    is_remote,
    MAX(hire_date) AS latest_hire_date,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
FROM employees
GROUP BY ROLLUP(manager_id, is_remote)
ORDER BY latest_hire_date
