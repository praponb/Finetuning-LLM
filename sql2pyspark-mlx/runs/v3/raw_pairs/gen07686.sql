SELECT id, full_name, email FROM employees WHERE UPPER(TRIM(email)) = 'JACK@STARTUP.IO' UNION SELECT id, full_name, email FROM employees WHERE NOT is_remote ORDER BY id
