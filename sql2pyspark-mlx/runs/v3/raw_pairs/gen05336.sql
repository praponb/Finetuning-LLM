SELECT *
FROM (
    SELECT
        DATE_TRUNC('MONTH', hire_date) hire_month,
        is_remote,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) level_values,
        MIN(salary) min_salary
    FROM employees
    GROUP BY DATE_TRUNC('MONTH', hire_date), is_remote
) agg
WHERE min_salary >= 63000
ORDER BY hire_month;
