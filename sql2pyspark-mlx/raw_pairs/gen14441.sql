WITH ValidEmployees AS (
    SELECT *, TRIM(level) AS clean_level
    FROM employees
    WHERE hire_date < '2024-06-01'
),
employees_stats AS (
    SELECT job_title, SUM(salary) AS total_salary, ROUND(AVG(salary), 2) AS avg_salary
    FROM ValidEmployees
    GROUP BY job_title
)
SELECT *
FROM employees_stats
ORDER BY avg_salary DESC, job_title;
