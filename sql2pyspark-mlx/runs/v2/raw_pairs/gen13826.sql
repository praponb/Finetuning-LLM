WITH base AS (
    SELECT *
    FROM employees
    WHERE MONTH(hire_date) IN (4, 8, 10) AND NOT is_remote AND salary <= 81000
),
summary AS (
    SELECT manager_id, COUNT(DISTINCT salary) unique_managers, SORT_ARRAY(COLLECT_SET(level)) level_list
    FROM base
    GROUP BY manager_id
)
SELECT *
FROM summary
WHERE unique_managers < 4
ORDER BY manager_id
