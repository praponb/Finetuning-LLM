SELECT email, DATE_SUB(signup_date, 11) AS signup_date_minus_11d FROM customers WHERE country = 'UK'
