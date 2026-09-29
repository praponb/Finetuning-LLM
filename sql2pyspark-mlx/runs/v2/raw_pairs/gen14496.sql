SELECT id, credit_limit, country, DATE_ADD(signup_date, 83) AS signup_date_plus_83d FROM customers WHERE LOWER(name) LIKE '%avi%' AND (country NOT IN ('DE', 'JP') OR LOWER(email) LIKE '%ran%')
