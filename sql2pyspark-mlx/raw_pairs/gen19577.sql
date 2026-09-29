WITH src AS (
    SELECT *
    FROM employees
    WHERE level IN ('L4', 'L2', 'L5') OR salary < 116000
),
salary_bucket_summary AS (
    SELECT CASE WHEN salary >= 120000 THEN 'high' WHEN salary >= 54000 THEN 'mid' ELSE 'low' END AS salary_bucket, COUNT(*) AS total_count, COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count
    FROM src
    GROUP BY CASE WHEN salary >= 120000 THEN 'high' WHEN salary >= 54000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM salary_bucket_summary
ORDER BY designer_count DESC
