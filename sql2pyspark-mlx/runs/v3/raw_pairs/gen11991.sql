select segment, credit_limit, date_trunc('YEAR', signup_date) as signup_date_period from customers;
