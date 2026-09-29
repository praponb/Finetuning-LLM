select date_trunc('MONTH', signup_date) as signup_month, avg(age) as avg_age from customers group by date_trunc('MONTH', signup_date) having max(credit_limit) < 11500
