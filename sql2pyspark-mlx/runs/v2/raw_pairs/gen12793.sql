SELECT
    manager_id,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values,
    MIN(hire_date) AS first_hire_date,
    MIN(salary) AS min_salary
FROM employees
WHERE manager_id IS NULL
GROUP BY manager_id
HAVING min_salary >= 82000;
