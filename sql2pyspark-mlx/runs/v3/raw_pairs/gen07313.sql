SELECT level, LOWER(email) AS email_lower FROM employees WHERE full_name LIKE '%smith%' AND NOT is_remote ORDER BY email_lower DESC
