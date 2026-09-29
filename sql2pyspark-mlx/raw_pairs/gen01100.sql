WITH src AS (
    SELECT *
    FROM employees
    WHERE manager_id IN (9, 18, 28, 33)
),
employees_stats AS (
    SELECT QUARTER(hire_date) AS hire_quarter, MAX(hire_date) AS last_hire_date, SORT_ARRAY(COLLECT_SET(level)) AS level_list
    FROM src
    GROUP BY QUARTER(hire_date)
)
SELECT *
FROM employees_stats
ORDER BY hire_quarter DESC;
