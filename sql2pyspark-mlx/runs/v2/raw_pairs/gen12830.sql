SELECT id, country, signup_date, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS signup_date_row FROM customers WHERE email LIKE '%@GMAIL.COM'
