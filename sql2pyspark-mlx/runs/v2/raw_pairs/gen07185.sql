WITH base AS (
    SELECT *
    FROM employees
    WHERE MONTH(hire_date) IN (8, 9, 11)
),
job_title_summary AS (
    SELECT job_title, is_remote, ROUND(AVG(salary), 2) AS avg_salary
    FROM base
    GROUP BY job_title, is_remote
)
SELECT job_title, is_remote, avg_salary
FROM job_title_summary
WHERE avg_salary < 24
