SELECT manager_id, full_name, DATE_SUB(hire_date, 4) AS hire_date_minus_4d
FROM employees
WHERE job_title IN ('Designer', 'Intern', 'Manager') AND is_remote
ORDER BY hire_date_minus_4d DESC, full_name;
