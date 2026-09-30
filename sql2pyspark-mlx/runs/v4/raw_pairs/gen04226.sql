WITH cleaned AS (
    SELECT
        *,
        COALESCE(email, 'unassigned') AS email_filled,
        CAST(salary AS INT) AS salary_int
    FROM employees
    WHERE hire_date > DATE '2023-05-28'
)
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    CASE WHEN salary >= 162000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    COUNT(CASE WHEN level = 'L1' THEN 1 END) AS l1_count,
    SUM(CASE WHEN job_title = 'Analyst' THEN salary_int ELSE 0 END) AS analyst_salary_int,
    ROUND(AVG(salary_int), 2) AS avg_salary_int,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    GROUPING(DATE_TRUNC('YEAR', hire_date)) AS grouping_id
FROM cleaned
GROUP BY CUBE (DATE_TRUNC('YEAR', hire_date), CASE WHEN salary >= 162000 THEN 'LARGE' ELSE 'SMALL' END);
