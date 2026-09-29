SELECT *, DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date, SUBSTRING(email, 1, 3) AS email_prefix FROM employees WHERE dept_id IN (21, 24) AND salary <= 87000;
