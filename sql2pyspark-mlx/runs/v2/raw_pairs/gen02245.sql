SELECT hire_date, manager_id, email
FROM employees
WHERE level IN ('L4', 'L1') AND (job_title IS NULL OR level IN ('L2', 'L4'));
