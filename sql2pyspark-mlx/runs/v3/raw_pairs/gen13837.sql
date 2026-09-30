WITH recent_employees AS (
    SELECT *
    FROM employees
    WHERE dept_id IN (11, 37) OR salary BETWEEN 114000 AND 121000
),
hire_month_summary AS (
    SELECT
        DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month,
        manager_id,
        SORT_ARRAY(COLLECT_SET(level)) AS level_list,
        ROUND(AVG(salary), 2) AS avg_salary
    FROM recent_employees
    GROUP BY DATE_FORMAT(hire_date, 'yyyy-MM'), manager_id
)
SELECT
    hire_month,
    manager_id,
    level_list,
    avg_salary
FROM hire_month_summary
ORDER BY manager_id DESC;
