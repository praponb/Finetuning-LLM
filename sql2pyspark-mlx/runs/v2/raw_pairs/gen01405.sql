WITH valid_employees AS (
    SELECT *
    FROM employees
    WHERE is_remote AND is_remote = FALSE AND dept_id IN (10, 18, 21)
),
Summary AS (
    SELECT
        DATE_TRUNC('MONTH', hire_date) AS hire_month,
        level,
        ROUND((100 * SUM(CASE WHEN job_title = 'Engineer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_engineer,
        SUM(CASE WHEN job_title = 'Engineer' THEN salary ELSE 0 END) AS engineer_salary
    FROM valid_employees
    GROUP BY DATE_TRUNC('MONTH', hire_date), level
)
SELECT
    hire_month,
    level,
    pct_engineer,
    engineer_salary
FROM Summary
