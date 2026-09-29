SELECT is_remote, full_name, SUBSTRING(email, 1, 2) AS email_prefix
FROM employees
WHERE LOWER(TRIM(email)) = 'carol@company.co' AND (NOT is_remote OR job_title IN ('Engineer', 'Designer'))
ORDER BY full_name DESC, email_prefix ASC;
