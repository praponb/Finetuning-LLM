SELECT QUARTER(signup_date) signup_quarter, MIN(signup_date) min_signup_date FROM customers WHERE NOT (country = 'SG' AND signup_date > '2023-11-01') GROUP BY QUARTER(signup_date)
