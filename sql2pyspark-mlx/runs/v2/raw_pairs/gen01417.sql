SELECT
    id,
    salary,
    full_name,
    SUBSTRING(job_title, 1, 3) AS job_title_prefix,
    CONCAT(full_name, ' ', level) AS full_name_level
FROM employees
WHERE MONTH(hire_date) IN (3, 6, 11) AND full_name IS NOT NULL AND level = 'L1';
