WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(TRIM(email)) AS email_normalized
    FROM employees
    WHERE hire_date > DATE '2023-08-02'
)
SELECT manager_id, job_title
FROM base
EXCEPT DISTINCT
SELECT manager_id, job_title
FROM employees
WHERE LOWER(full_name) LIKE '%oah%';
