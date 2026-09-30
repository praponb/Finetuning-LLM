SELECT * FROM employees WHERE full_name NOT LIKE '%MILLE%' AND job_title NOT IN ('Manager', 'Engineer', 'Director') AND salary > 125000
