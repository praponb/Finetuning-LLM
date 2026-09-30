SELECT
    id,
    job_title,
    hire_date,
    CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
FROM employees
WHERE email LIKE '%@mail.co.th'
ORDER BY id DESC, is_remote_label DESC;
