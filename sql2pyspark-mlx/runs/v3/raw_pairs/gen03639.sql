SELECT id, email, country, city, ADD_MONTHS(signup_date, 6) AS signup_date_plus_months, COALESCE(age, 0) AS age_filled FROM customers
