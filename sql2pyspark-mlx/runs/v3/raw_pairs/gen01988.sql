WITH recent_employees AS (
    SELECT *
    FROM employees
    WHERE manager_id = 7 OR dept_id IN (18, 34)
),
Summary AS (
    SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END hire_cohort, is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) level_values
    FROM recent_employees
    GROUP BY CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END, is_remote
)
SELECT hire_cohort, level_values
FROM Summary
ORDER BY level_values DESC, hire_cohort;
