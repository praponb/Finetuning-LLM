select id, signup_date, name, country, round(credit_limit / age, 2) as credit_limit_per_age from customers;
