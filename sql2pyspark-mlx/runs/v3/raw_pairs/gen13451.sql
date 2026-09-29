SELECT id, job_title, full_name, LENGTH(full_name) AS full_name_length FROM employees WHERE YEAR(hire_date) = 2024 AND manager_id = 39 AND full_name NOT LIKE '%FRANK%' ORDER BY full_name;
