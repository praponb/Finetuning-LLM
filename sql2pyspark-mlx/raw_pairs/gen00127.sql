WITH base AS (
    SELECT *, LENGTH(full_name) AS full_name_length
    FROM employees
    WHERE (dept_id IN (22, 27, 28, 36) AND dept_id IN (18, 19)) OR dept_id = 12
),
hire_month_summary AS (
    SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END) AS l4_count
    FROM base
    GROUP BY DATE_TRUNC('MONTH', hire_date)
)
SELECT hire_month, l4_count
FROM hire_month_summary
WHERE l4_count < 22
ORDER BY hire_month
