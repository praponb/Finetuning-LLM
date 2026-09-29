SELECT *, LAST_DAY(hire_date) hire_date_month_end
FROM employees
WHERE full_name LIKE 'Jac%' OR job_title = 'Director'
