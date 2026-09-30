SELECT id, is_remote, SUBSTRING(email, 1, 3) AS email_prefix FROM employees WHERE salary BETWEEN 103000 AND 107000 AND email NOT LIKE '%@GMAIL.COM';
