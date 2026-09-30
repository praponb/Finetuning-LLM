SELECT
    salary_bucket,
    job_title,
    first_hire_date,
    unique_depts
FROM (
    SELECT
        CASE WHEN salary >= 159000 THEN 'high' WHEN salary >= 51000 THEN 'mid' ELSE 'low' END AS salary_bucket,
        job_title,
        MIN(hire_date) AS first_hire_date,
        COUNT(DISTINCT salary) AS unique_depts
    FROM employees
    GROUP BY CASE WHEN salary >= 159000 THEN 'high' WHEN salary >= 51000 THEN 'mid' ELSE 'low' END, job_title
) AS t
WHERE unique_depts < 5
