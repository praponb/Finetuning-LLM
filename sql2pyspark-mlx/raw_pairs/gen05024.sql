SELECT email, level, SPLIT(LOWER(email), '@')[0] AS email_user FROM employees WHERE email LIKE '%@mail.co.th' AND LOWER(TRIM(email)) = 'liam@outlook.com'
