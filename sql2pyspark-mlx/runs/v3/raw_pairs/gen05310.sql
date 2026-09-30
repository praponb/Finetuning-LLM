WITH src AS (
    SELECT *
    FROM employees
    WHERE level = 'L2' AND email IS NULL
),
job_title_summary AS (
    SELECT job_title, MIN(hire_date) AS first_hire_date, SUM(CASE WHEN level = 'L4' THEN salary ELSE 0 END) AS l4_salary
    FROM src
    GROUP BY job_title
)
SELECT *
FROM job_title_summary
WHERE l4_salary > 36
ORDER BY job_title, first_hire_date;
