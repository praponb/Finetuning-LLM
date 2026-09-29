SELECT
    id,
    job_title,
    CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label,
    ROUND(salary * 1.07, 2) AS salary_scaled
FROM employees;
