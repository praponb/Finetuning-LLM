SELECT
    id,
    salary,
    email,
    CONCAT(job_title, ', ', email) AS job_title_email,
    CASE WHEN salary >= 126000 THEN 'High' WHEN salary >= 74000 THEN 'Medium' ELSE 'Low' END AS salary_band
FROM employees
WHERE YEAR(hire_date) >= 2024;
