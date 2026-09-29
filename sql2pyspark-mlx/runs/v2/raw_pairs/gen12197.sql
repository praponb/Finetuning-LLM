SELECT MAX(signup_date) AS latest_signup_date, MAX(age) AS max_age, MAX(credit_limit) AS max_credit_limit FROM customers ORDER BY max_credit_limit DESC;
