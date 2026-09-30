SELECT
    job_title,
    DATE_TRUNC('MONTH', hire_date) AS hire_month,
    COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count,
    SUM(CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END) AS analyst_count
FROM employees
GROUP BY 1, 2
