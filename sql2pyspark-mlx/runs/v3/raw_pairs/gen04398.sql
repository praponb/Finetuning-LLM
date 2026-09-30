WITH base AS (
    SELECT *
    FROM employees
    WHERE hire_date > '2025-10-09' AND job_title NOT IN ('Designer', 'Analyst', 'Manager')
),
hire_month_summary AS (
    SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, SUM(salary) AS total_salary
    FROM base
    GROUP BY DATE_TRUNC('MONTH', hire_date)
)
SELECT *
FROM hire_month_summary;
