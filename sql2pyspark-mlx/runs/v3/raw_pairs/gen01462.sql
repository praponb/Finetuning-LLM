WITH src AS (
    SELECT *, COALESCE(email, 'unassigned') AS email_filled
    FROM employees
    WHERE (manager_id IN (1, 18) AND salary <= 77000) OR email LIKE '%@GMAIL.COM'
),
Agg AS (
    SELECT is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values, ROUND(AVG(salary), 2) AS avg_salary
    FROM src
    GROUP BY is_remote
)
SELECT is_remote, job_title_values, avg_salary
FROM Agg
WHERE avg_salary > 77
ORDER BY job_title_values
