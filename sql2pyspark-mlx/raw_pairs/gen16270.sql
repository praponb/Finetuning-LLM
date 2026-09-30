WITH FilteredEmployees AS (
    SELECT *, SPLIT(LOWER(email), '@')[0] AS email_user
    FROM employees
    WHERE UPPER(email) LIKE '%ENR%' AND full_name IS NOT NULL AND hire_date BETWEEN '2024-04-14' AND '2025-07-26'
),
agg AS (
    SELECT CASE WHEN level IN ('L5', 'L1') THEN 'core' ELSE 'other' END AS level_group, job_title, COUNT(DISTINCT salary) AS unique_managers
    FROM FilteredEmployees
    GROUP BY CASE WHEN level IN ('L5', 'L1') THEN 'core' ELSE 'other' END, job_title
)
SELECT level_group, job_title
FROM agg
