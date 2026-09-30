SELECT hire_date, job_title, INITCAP(TRIM(full_name)) full_name_title
FROM employees
WHERE UPPER(email) LIKE '%MMA%' OR job_title != 'Analyst'
