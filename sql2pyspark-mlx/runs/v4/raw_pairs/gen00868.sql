SELECT id, hire_date, INITCAP(TRIM(email)) AS email_title
FROM employees
WHERE hire_date <= '2023-11-05' AND (salary <= 133000 OR level = 'L2')
ORDER BY 2, email_title
