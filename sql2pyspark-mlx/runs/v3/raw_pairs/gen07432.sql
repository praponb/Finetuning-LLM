SELECT *, DATE_ADD(signup_date, 37) AS signup_date_plus_37d, SPLIT(LOWER(email), '@')[0] AS email_user FROM customers WHERE signup_date <= '2023-06-27' AND age < 43 AND name LIKE '%Ivy%' ORDER BY 11;
