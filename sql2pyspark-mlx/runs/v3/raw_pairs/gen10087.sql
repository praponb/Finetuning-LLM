SELECT *, CASE WHEN salary >= 134000 THEN 'High' WHEN salary >= 81000 THEN 'Medium' ELSE 'Low' END AS salary_level, CONCAT_WS('-', email, full_name) AS email_full_name_key
FROM employees
WHERE job_title != 'Engineer' AND (dept_id IN (10, 17, 26, 40) OR full_name IS NOT NULL)
