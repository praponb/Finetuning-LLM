SELECT signup_date, credit_limit, is_active, SPLIT(LOWER(email), '@')[0] AS email_user FROM customers
