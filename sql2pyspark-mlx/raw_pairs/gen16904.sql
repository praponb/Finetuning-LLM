SELECT
    id,
    dept_id,
    email,
    INITCAP(TRIM(job_title)) AS job_title_title,
    DATE_FORMAT(hire_date, 'yyyy-MM-dd') AS hire_date_label
FROM employees
ORDER BY hire_date_label ASC, id ASC
LIMIT 5;
