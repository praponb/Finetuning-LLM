select id, signup_date, segment, substring(email, 1, 3) as email_prefix from customers where credit_limit <= 9000 and email like '%@startup.io'
